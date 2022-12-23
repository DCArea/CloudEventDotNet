using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

internal sealed class CloudEventPubSub : ICloudEventPubSub
{
    private readonly PubSubOptions _options;
    private readonly Dictionary<string, ICloudEventPublisher> _publishers;
    private readonly ILogger<CloudEventPubSub> _logger;
    private readonly Registry _registry;

    public CloudEventPubSub(
        ILogger<CloudEventPubSub> logger,
        IServiceProvider services,
        Registry registry,
        IOptions<PubSubOptions> options)
    {
        _options = options.Value;
        _publishers = _options.PublisherFactoris.ToDictionary(kvp => kvp.Key, kvp => kvp.Value(services));
        _logger = logger;
        _registry = registry;
    }

    public Task<CloudEvent<TData>> PublishAsync<TData>(TData data)
    {
        var dataType = typeof(TData);
        var metadata = _registry.GetMetadata(dataType);
        return PublishAsync(data, metadata);
    }

    public async Task<CloudEvent<TData>> PublishAsync<TData>(TData data, CloudEventMetadata metadata)
    {
        var cloudEvent = new CloudEvent<TData>(
            Id: Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: null
        );
        using var activity = CloudEventPublishTelemetry.OnCloudEventPublishing(metadata, cloudEvent, _logger);
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);
        CloudEventPublishTelemetry.OnCloudEventPublished(metadata);
        return cloudEvent;
    }

}
