
using System.Diagnostics;
using CloudEventKafkaTester;
using Confluent.Kafka;
using DCA.DotNet.Extensions.CloudEvents;

string broker = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:9092";
string topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "devperftest";
string consumerGroup = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP") ?? "devperftest";
var autoOffsetReset = Environment.GetEnvironmentVariable("KAFKA_AUTO_OFFSET_RESET") switch
{
    "latest" => AutoOffsetReset.Latest,
    "earliest" => AutoOffsetReset.Earliest,
    _ => AutoOffsetReset.Latest
};


var services = new ServiceCollection()
// .AddLogging();
.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Information));
// .AddLogging(logging => logging.AddConsole().AddFilter((category, level) => category.Contains("TopicPartitionChannel")));
// .AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));
services.AddCloudEvents(defaultPubSubName: "kafka", defaultTopic: topic)
    .Load(typeof(Ping).Assembly)
    .AddKafkaPubSub("kafka", options =>
    {
        options.ProducerConfig = new ProducerConfig
        {
            BootstrapServers = broker,
            Acks = Acks.Leader,
        };
    }, options =>
    {
        options.ConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = consumerGroup,
            AutoOffsetReset = autoOffsetReset,
        };
        // options.RunningWorkItemLimit = -1;
        options.DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce;
    });

var sp = services.BuildServiceProvider();
sp.GetRequiredService<Registry>().Debug();

string scenario = args.Length > 0 ? args[0] : "validate";

Console.WriteLine($"Starting {scenario}");
var parallelism = args.Length > 1 ? int.Parse(args[1]) : 1;
var count = args.Length > 2 ? int.Parse(args[2]) : 1000;

var publishTask = scenario.ToLower() switch
{
    "validate" => Validate(),
    "publish" => Publish(),
    _ => throw new ArgumentException($"Unknown scenario: {scenario}")
};

await publishTask;

await Subscribe();

async Task Validate()
{
    var pubsub = sp.GetRequiredService<ICloudEventPubSub>();
    await pubsub.PublishAsync(new Ping());
    Console.WriteLine("Published 1");
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
    await Monitor(timer);
    await subscriber.StopAsync(default);
}

async Task Monitor(PeriodicTimer timer)
{
    long lastCount = 0L;
    while (await timer.WaitForNextTickAsync())
    {
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
}
