
using System.Diagnostics;
using CloudEventDotNet;
using CloudEventRedisTester;
using StackExchange.Redis;

string redisConnectionString = Environment.GetEnvironmentVariable("CONNSTR") ?? "localhost:6379";
string topic = Environment.GetEnvironmentVariable("TOPIC") ?? "devperftest";
string consumerGroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "devperftest";
int runningWorkItemLimit = int.Parse(Environment.GetEnvironmentVariable("RUNNING_WORK_ITEM_LIMIT") ?? "1024");
int maxLength = int.Parse(Environment.GetEnvironmentVariable("MAX_LENGTH") ?? (100_000_000).ToString());

var services = new ServiceCollection()
// .AddLogging();
.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Information));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Trace));

var redis = ConnectionMultiplexer.Connect(redisConnectionString);

services.AddCloudEvents(defaultPubSubName: "redis", defaultTopic: topic)
    .Load(typeof(Ping).Assembly)
    .AddRedisPubSub("redis", options =>
    {
        options.ConnectionMultiplexerFactory = () => redis;
        options.MaxLength = maxLength;
    }, options =>
    {
        options.ConnectionMultiplexerFactory = () => redis;
        options.ConsumerGroup = consumerGroup;
        options.RunningWorkItemLimit = runningWorkItemLimit;
    });

var sp = services.BuildServiceProvider();
sp.GetRequiredService<Registry>().Debug();

string scenario = args[0];

int parallelism = int.Parse(args[1]);
int count = int.Parse(args[2]);
if (scenario == "pub" || scenario == "pubsub")
{
    Console.WriteLine($"Starting publish");
    await Publish();
}

if (scenario == "sub" || scenario == "pubsub")
{
    Console.WriteLine($"Starting subscribe");
    await Subscribe();
}

async Task Publish()
{
    var pubsub = sp.GetRequiredService<ICloudEventPubSub>();
    var tasks = new List<Task>();
    var sw = Stopwatch.StartNew();
    for (int i = 0; i < parallelism; i++)
    {
        var task = Task.Run(async () =>
        {
            for (int j = 0; j < count; j++)
            {
                await pubsub.PublishAsync(new Ping());
            }
        });
        tasks.Add(task);
    }
    await Task.WhenAll(tasks);
    sw.Stop();
    Console.WriteLine($"Published {parallelism} * {count}, rate: {count * parallelism / sw.Elapsed.TotalSeconds:F2}/s");
}

async Task Subscribe()
{
    Console.WriteLine("Subscribing");

    var subscriber = (SubscribeHostedService)sp.GetRequiredService<IHostedService>();
    await subscriber.StartAsync(default);

    var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    _ = Task.Run(() => Monitor(timer));
    Console.ReadKey();
    await subscriber.StopAsync(default);
}

async Task Monitor(PeriodicTimer timer)
{
    long lastCount = 0L;
    int seconds = 0;
    while (await timer.WaitForNextTickAsync())
    {
        seconds++;
        var currentCount = PingHandler.Count;
        var delta = currentCount - lastCount;
        Console.WriteLine($"Processed: {currentCount}, rate: {delta / 1.0:F1}/s");
        lastCount = currentCount;
        if (currentCount >= count * parallelism)
        {
            timer.Dispose();
            await Task.Delay(TimeSpan.FromSeconds(5));
            break;
        }
    }

    Console.WriteLine($"Rate: {PingHandler.Count / seconds:F1}/s");
}
