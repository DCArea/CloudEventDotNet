using System.Diagnostics;
using CloudEventDotNet;
using Polly;

namespace CloudEventTester;

public class DeadLetterTester : Tester
{
    private ServiceProvider _sp = default!;
    private int _parallelism = default!;
    private int _count = default!;
    private string _action = default!;

    protected override PubSubBuilder ConfigureKafka()
    {
        var builder = base.ConfigureKafka();
        return builder.AddPubSubDeadLetterSender(opts =>
        {
            opts.Topic = "DL";
        }).AddDefaultResiliencePipeline(b =>
        {
            b.AddRetry(new());
        });
    }

    protected override PubSubBuilder ConfigureRedis()
    {
        var builder = base.ConfigureRedis();
        return builder.AddPubSubDeadLetterSender(opts =>
        {
            opts.Topic = "DL";
        }).AddDefaultResiliencePipeline(b =>
        {
            b.AddRetry(new());
        });
    }


    public override async Task RunAsync(string[] args)
    {
        _action = args[1];
        _parallelism = int.Parse(args[2]);
        _count = int.Parse(args[3]);
        _sp = Services.BuildServiceProvider();

        if (_action is "pub" or "pubsub")
        {
            Console.WriteLine($"Starting publish");
            await Publish();
        }

        if (_action is "sub" or "pubsub")
        {
            Console.WriteLine($"Starting subscribe");
            await Subscribe();
        }
    }


    private async Task Publish()
    {
        ICloudEventPubSub pubsub = _sp.GetRequiredService<ICloudEventPubSub>();
        var tasks = new List<Task>();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < _parallelism; i++)
        {
            var task = Task.Run(async () =>
            {
                for (int j = 0; j < _count; j++)
                {
                    await pubsub.PublishAsync(new DLTest());
                }
            });
            tasks.Add(task);
        }
        await Task.WhenAll(tasks);
        sw.Stop();
        Console.WriteLine($"Published {_parallelism} * {_count}, rate: {_count * _parallelism / sw.Elapsed.TotalSeconds:F2}/s");
    }

    private async Task Subscribe()
    {
        Console.WriteLine("Subscribing");

        var subscriber = (SubscribeHostedService)_sp.GetRequiredService<IHostedService>();
        await subscriber.StartAsync(default);

        var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        _ = Task.Run(() => Monitor(timer));
        _ = Console.ReadKey();
        await subscriber.StopAsync(default);
    }

    private async Task Monitor(PeriodicTimer timer)
    {
        long lastCount1 = 0L;
        long lastCount2 = 0L;
        int seconds = 0;
        while (await timer.WaitForNextTickAsync())
        {
            seconds++;
            long count1 = DLTestHandler.Count;
            long count2 = DLTestDeadLetterHandler.Count;
            long delta1 = count1 - lastCount1;
            long delta2 = count2 - lastCount2;
            Console.WriteLine($"Processed: {count1}, rate: {delta1 / 1.0:F1}/s");
            Console.WriteLine($"Processed DL: {count2}, rate: {delta2 / 1.0:F1}/s");
            lastCount1 = count1;
            lastCount2 = count2;
            if (count2 >= _count * _parallelism)
            {
                timer.Dispose();
                await Task.Delay(TimeSpan.FromSeconds(5));
                break;
            }
        }

        Console.WriteLine($"Rate: {lastCount1 / seconds:F1}/s, {lastCount2 / seconds:F1}/s");
    }
}
