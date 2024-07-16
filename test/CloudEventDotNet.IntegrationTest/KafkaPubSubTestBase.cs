using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CloudEventDotNet.IntegrationTest;

public abstract class PubSubTestBase
{
    public PubSubTestBase()
    {
        var services = new ServiceCollection();

        ConfigureServices(services);
        ConfigurePubSub(services);

        ServiceProvider = services.BuildServiceProvider();
    }

    public ServiceProvider ServiceProvider { get; }
    private SubscribeHostedService Subscriber => (SubscribeHostedService)ServiceProvider.GetRequiredService<IHostedService>();
    public ICloudEventPubSub PubSub => ServiceProvider.GetRequiredService<ICloudEventPubSub>();
    internal SubscriptionMonitor<T> GetMonitor<T>() => ServiceProvider.GetRequiredService<SubscriptionMonitor<T>>();

    protected virtual void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton(typeof(SubscriptionMonitor<>));
        services.AddLogging();
    }

    protected abstract void ConfigurePubSub(IServiceCollection services);

    protected virtual async Task StartAsync()
    {
        await Subscriber.StartAsync(default)
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

    protected virtual async Task StopAsync()
    {
        await Subscriber.StopAsync(default)
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

}
