using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

/// <summary>
/// A hosted service to pull CloudEvents from subscribed topics
/// </summary>
public class SubscribeHostedService(
    ILogger<SubscribeHostedService> logger,
    PubSubOptions options) : IHostedService
{
    private readonly List<ICloudEventSubscriber> _subscribers = [.. options.Subscribers.Values];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting subscribers");
        await Task.WhenAll(_subscribers.Select(s => s.StartAsync()));
        logger.LogInformation("Started subscribers");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stoping subscribers");
        await Task.WhenAll(_subscribers.Select(s => s.StopAsync()));
        logger.LogInformation("Stopped subscribers");
    }
}
