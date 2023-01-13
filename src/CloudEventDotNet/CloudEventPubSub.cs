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

    public Task<CloudEvent<TData>> PublishAsync<TData>(TData data,
        string? id = null,
        DateTimeOffset? time = null,
        string? subject = null)
    {
        var dataType = typeof(TData);
        var metadata = _registry.GetMetadata(dataType);
        var cloudEvent = new CloudEvent<TData>(
            Id: id ?? Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: time ?? DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: subject
        );
        return PublishAsync(cloudEvent, metadata);
    }

    public Task<CloudEvent<TData>> PublishAsync<TData>(
        CloudEvent<TData> cloudEvent,
        string? pubsubName = null,
        string? topic = null)
    {
        var metadata = new CloudEventMetadata(
            pubsubName ?? _options.DefaultPubSubName,
            topic ?? _options.DefaultTopic,
            cloudEvent.Type,
            cloudEvent.Source
        );
        return PublishAsync(cloudEvent, metadata);
    }

    private async Task<CloudEvent<TData>> PublishAsync<TData>(CloudEvent<TData> cloudEvent, CloudEventMetadata metadata)
    {
        using var activity = CloudEventPublishTelemetry.OnCloudEventPublishing(metadata, cloudEvent, _logger);
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);
        CloudEventPublishTelemetry.OnCloudEventPublished(metadata);
        return cloudEvent;
    }

}
