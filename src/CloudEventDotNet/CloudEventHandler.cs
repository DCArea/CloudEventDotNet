using System.Diagnostics;
using CloudEventDotNet.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;

namespace CloudEventDotNet;

public enum ProcessingResult
{
    Success = 0,
    SentToDeadLetter = 1,
    Failed = 2
}

public interface ICloudEventHandler
{
    Task<ProcessingResult> ProcessAsync(CloudEvent @event, CancellationToken token);
}

internal sealed class CloudEventHandler(
    CloudEventMetadata metadata,
    HandleCloudEventDelegate handleDelegate,
    IServiceProvider serviceProvider,
    IServiceScopeFactory scopeFactory,
    ILogger<CloudEventHandler> logger) : ICloudEventHandler
{
    public const string ResiliencePolicyName = nameof(CloudEventHandler);
    private readonly CloudEventMetricsContext _metrics = new(metadata.PubSubName, metadata.Topic, metadata.Type);
    private readonly CloudEventLogger _logger = new(logger, metadata);
    private readonly ResiliencePipeline? _resiliencePipeline;

    public CloudEventHandler(
    ResiliencePipeline resiliencePipeline,
    CloudEventMetadata metadata,
    HandleCloudEventDelegate handleDelegate,
    IServiceProvider serviceProvider,
    IServiceScopeFactory scopeFactory,
    ILogger<CloudEventHandler> logger) : this(metadata, handleDelegate, serviceProvider, scopeFactory, logger)
    {
        _resiliencePipeline = resiliencePipeline;
    }

    public async Task<ProcessingResult> ProcessAsync(CloudEvent @event, CancellationToken token)
    {
        var ac = Activity.Current;
        try
        {
            using var scope = scopeFactory.CreateScope();
            var processingAt = DateTimeOffset.UtcNow;
            var deliveryTardiness = processingAt - @event.Time;
            _metrics.DeliveryTardiness.Record((long)deliveryTardiness.TotalMilliseconds);
            _logger.CloudEventProcessing(@event.Id, deliveryTardiness);
            ac?.SetTag("cloudevents.tardiness", deliveryTardiness.TotalSeconds);
            var sw = ValueStopwatch.StartNew();

            if (_resiliencePipeline != null)
            {
                await _resiliencePipeline.ExecuteAsync((ct) => new ValueTask(handleDelegate(scope.ServiceProvider, @event, ct)), token).ConfigureAwait(false);
            }
            else
            {
                await handleDelegate(scope.ServiceProvider, @event, token).ConfigureAwait(false);
            }

            _metrics.ProcessLatency.Record((long)sw.GetElapsedTime().TotalMilliseconds);
            _logger.CloudEventProcessed(@event.Id);
            Activity.Current?.SetStatus(ActivityStatusCode.Ok);
            return ProcessingResult.Success;
        }
        catch (Exception ex)
        {
            _logger.CloudEventProcessFailed(ex, @event.Id);
            Activity.Current?.SetStatus(ActivityStatusCode.Error, $"Exception: {ex.GetType().Name}");

            var _deadLetterSender = serviceProvider.GetService<IDeadLetterSender>();
            if (_deadLetterSender != null)
            {
                await _deadLetterSender.SendAsync(metadata, @event, ex.ToString());
                return ProcessingResult.SentToDeadLetter;
            }
            return ProcessingResult.Failed;
        }
    }
}
