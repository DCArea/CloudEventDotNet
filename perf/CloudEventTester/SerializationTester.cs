using CloudEventDotNet;

namespace CloudEventKafkaTester;

public class SerializationTester : Tester
{
    public override async Task RunAsync(string[] args)
    {
        var sp = Services.BuildServiceProvider();

        Console.WriteLine($"Starting publish");
        var pubsub = sp.GetRequiredService<ICloudEventPubSub>();
        await pubsub.PublishAsync(new SerializationTest(Foo: "bar"));

        Console.WriteLine("Subscribing");
        var subscriber = (SubscribeHostedService)sp.GetRequiredService<IHostedService>();
        await subscriber.StartAsync(default);
        Console.ReadKey();
        await subscriber.StopAsync(default);
    }
}
