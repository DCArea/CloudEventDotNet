using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

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

    public async Task<bool> ProcessAsync(CloudEvent @event, CancellationToken token)
    {
        using var activity = _telemetry.OnProcessing(@event);
        try
        {
            using var scope = _scopeFactory.CreateScope();
            await _process(scope.ServiceProvider, @event, token).ConfigureAwait(false);
            _telemetry.OnCloudEventProcessed(@event);
            return true;
        }
        catch (Exception ex)
        {
            _telemetry.OnProcessingCloudEventFailed(ex, @event.Id);
            return false;
        }
    }
}
