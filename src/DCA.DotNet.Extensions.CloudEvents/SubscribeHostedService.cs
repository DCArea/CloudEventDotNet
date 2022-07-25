using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DCA.DotNet.Extensions.CloudEvents;

public class SubscribeHostedService : IHostedService
{
    private readonly PubSubOptions _options;
    private readonly List<ICloudEventSubscriber> _subscribers;
    private readonly ILogger<SubscribeHostedService> _logger;

    public SubscribeHostedService(
        ILogger<SubscribeHostedService> logger,
        IServiceProvider serviceProvider,
        IOptions<PubSubOptions> options)
    {
        _options = options.Value;
        _subscribers = _options
            .SubscriberFactoris.Values
            .Select(factory => factory(serviceProvider))
            .ToList();
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting subscribers");
        var tasks = new List<Task>();
        await Task.WhenAll(_subscribers.Select(s => s.StartAsync()));
        _logger.LogInformation("Started subscribers");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stoping subscribers");
        await Task.WhenAll(_subscribers.Select(s => s.StopAsync()));
        _logger.LogInformation("Stopped subscribers");
    }
}
