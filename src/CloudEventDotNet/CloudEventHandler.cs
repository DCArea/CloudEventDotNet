using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal class CloudEventHandler
{
    private readonly HandleCloudEventDelegate _process;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly CloudEventProcessingTelemetry _telemetry;

    public CloudEventHandler(
        CloudEventMetadata metadata,
        HandleCloudEventDelegate handleDelegate,
        ILoggerFactory loggerFactory,
        IServiceScopeFactory scopeFactory)
    {
        _process = handleDelegate;
        _scopeFactory = scopeFactory;
        _telemetry = new CloudEventProcessingTelemetry(loggerFactory, metadata);
    }

    public async Task<bool> ProcessAsync(CloudEvent cloudEvent, CancellationToken token)
    {
        try
        {
            _telemetry.OnProcessing(cloudEvent);
            using var scope = _scopeFactory.CreateScope();
            await _process(scope.ServiceProvider, cloudEvent, token).ConfigureAwait(false);
            _telemetry.OnCloudEventProcessed(cloudEvent);
            return true;
        }
        catch (Exception ex)
        {
            _telemetry.OnProcessingCloudEventFailed(ex, cloudEvent.Id);
            return false;
        }
    }

    public Activity? StartProcessing(CloudEvent @event) => _telemetry.OnProcessing(@event);
}
