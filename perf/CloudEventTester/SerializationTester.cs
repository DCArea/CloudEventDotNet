using CloudEventDotNet;

namespace CloudEventTester;

public class SerializationTester : Tester
{
    public override async Task RunAsync(string[] args)
    {
        ServiceProvider sp = Services.BuildServiceProvider();

        Console.WriteLine($"Starting publish");
        ICloudEventPubSub pubsub = sp.GetRequiredService<ICloudEventPubSub>();
        await pubsub.PublishAsync(new SerializationTest(Foo: "bar"));

        Console.WriteLine("Subscribing");
        var subscriber = (SubscribeHostedService)sp.GetRequiredService<IHostedService>();
        await subscriber.StartAsync(default);
        _ = Console.ReadKey();
        await subscriber.StopAsync(default);
    }
}
