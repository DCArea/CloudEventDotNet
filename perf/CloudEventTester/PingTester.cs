using System.Diagnostics;
using CloudEventDotNet;

namespace CloudEventTester;

public class PingTester : Tester
{
    private ServiceProvider _sp = default!;
    private int _parallelism = default!;
    private int _count = default!;
    private string _action = default!;

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
                    await pubsub.PublishAsync(new Ping());
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
        long lastCount = 0L;
        int seconds = 0;
        while (await timer.WaitForNextTickAsync())
        {
            seconds++;
            long currentCount = PingHandler.Count;
            long delta = currentCount - lastCount;
            Console.WriteLine($"Processed: {currentCount}, rate: {delta / 1.0:F1}/s");
            lastCount = currentCount;
            if (currentCount >= _count * _parallelism)
            {
                timer.Dispose();
                await Task.Delay(TimeSpan.FromSeconds(5));
                break;
            }
        }

        Console.WriteLine($"Rate: {PingHandler.Count / seconds:F1}/s");
    }
}
