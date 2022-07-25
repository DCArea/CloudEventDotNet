
using System.Diagnostics;
using CloudEventKafkaTester;
using Confluent.Kafka;
using CloudEventDotNet;

string broker = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:9092";
string topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "devperftest";
string consumerGroup = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP") ?? "devperftest";
int runningWorkItemLimit = int.Parse(Environment.GetEnvironmentVariable("KAFKA_RUNNING_WORK_ITEM_LIMIT") ?? "128");
var autoOffsetReset = Environment.GetEnvironmentVariable("KAFKA_AUTO_OFFSET_RESET") switch
{
    "latest" => AutoOffsetReset.Latest,
    "earliest" => AutoOffsetReset.Earliest,
    _ => AutoOffsetReset.Latest
};
var deliveryGuarantee = Environment.GetEnvironmentVariable("KAFKA_DELIVERY_GUARANTEE") switch
{
    "at_least_once" => DeliveryGuarantee.AtLeastOnce,
    "at_most_once" => DeliveryGuarantee.AtMostOnce,
    _ => DeliveryGuarantee.AtLeastOnce
};

var services = new ServiceCollection()
// .AddLogging();
.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Information));
// .AddLogging(logging => logging.AddConsole().AddFilter((category, level) => category.Contains("TopicPartitionChannel")));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Trace));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Warning));
services.AddCloudEvents(defaultPubSubName: "kafka", defaultTopic: topic)
    .Load(typeof(Ping).Assembly)
    .AddKafkaPubSub("kafka", options =>
    {
        options.ProducerConfig = new ProducerConfig
        {
            BootstrapServers = broker,
            Acks = Acks.Leader,
            LingerMs = 10
        };
    }, options =>
    {
        options.ConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = consumerGroup,
            AutoOffsetReset = autoOffsetReset,

            QueuedMinMessages = 300_000,
            FetchWaitMaxMs = 1_000
        };
        options.RunningWorkItemLimit = runningWorkItemLimit;
        options.DeliveryGuarantee = deliveryGuarantee;
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
            for (var j = 0; j < count; j++)
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
