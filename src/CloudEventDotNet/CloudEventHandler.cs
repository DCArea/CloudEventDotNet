using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal class CloudEventHandler
{
    private readonly CloudEventMetadata _metadata;
    private readonly HandleCloudEventDelegate _process;
    private readonly IServiceProvider _serviceProvider;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly CloudEventProcessingTelemetry _telemetry;

    public CloudEventHandler(
        CloudEventMetadata metadata,
        HandleCloudEventDelegate handleDelegate,
        ILoggerFactory loggerFactory,
        IServiceProvider serviceProvider,
        IServiceScopeFactory scopeFactory)
    {
        _metadata = metadata;
        _process = handleDelegate;
        this._serviceProvider = serviceProvider;
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
