using System.Diagnostics;
using CloudEventDotNet.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

public interface ICloudEventHandler
{
    Task<bool> ProcessAsync(CloudEvent @event, CancellationToken token);
}

internal sealed class CloudEventHandler : ICloudEventHandler
{
    private readonly CloudEventMetadata _metadata;
    private readonly HandleCloudEventDelegate _process;
    private readonly IServiceProvider _serviceProvider;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly CloudEventMetricsContext _metrics;
    private readonly CloudEventLogger _logger;

    public CloudEventHandler(
        CloudEventMetadata metadata,
        HandleCloudEventDelegate handleDelegate,
        IServiceProvider serviceProvider,
        IServiceScopeFactory scopeFactory,
        ILogger<CloudEventHandler> logger)
    {
        _metadata = metadata;
        _process = handleDelegate;
        _serviceProvider = serviceProvider;
        _scopeFactory = scopeFactory;
        _metrics = new CloudEventMetricsContext(metadata.PubSubName, metadata.Topic, metadata.Type);
        _logger = new CloudEventLogger(logger, metadata);

    }

    public async Task<bool> ProcessAsync(CloudEvent @event, CancellationToken token)
    {
        var ac = Activity.Current;
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var processingAt = DateTimeOffset.UtcNow;
            var deliveryTardiness = processingAt - @event.Time;
            _metrics.DeliveryTardiness.Record((long)deliveryTardiness.TotalMilliseconds);
            _logger.CloudEventProcessing(@event.Id, deliveryTardiness);
            ac?.SetTag("cloudevents.tardiness", deliveryTardiness.TotalSeconds);
            var sw = ValueStopwatch.StartNew();
            await _process(scope.ServiceProvider, @event, token).ConfigureAwait(false);
            _metrics.ProcessLatency.Record((long)sw.GetElapsedTime().TotalMilliseconds);
            _logger.CloudEventProcessed(@event.Id);
            Activity.Current?.SetStatus(ActivityStatusCode.Ok);
            return true;
        }
        catch (Exception ex)
        {
            _logger.CloudEventProcessFailed(ex, @event.Id);
            Activity.Current?.SetStatus(ActivityStatusCode.Error, $"Exception: {ex.GetType().Name}");

            var _deadLetterSender = _serviceProvider.GetService<IDeadLetterSender>();
            if (_deadLetterSender != null)
            {
                await _deadLetterSender.SendAsync(_metadata, @event, ex.ToString());
                return true;
            }
            return false;
        }
    }
}
