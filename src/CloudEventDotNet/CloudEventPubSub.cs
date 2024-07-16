using System.Collections.Frozen;
using CloudEventDotNet.Telemetry;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal sealed class CloudEventPubSub : ICloudEventPubSub
{
    private readonly PubSubOptions _options;
    private readonly FrozenDictionary<string, ICloudEventPublisher> _publishers;
    private readonly ILogger<CloudEventPubSub> _logger;
    private readonly Registry2 _registry;

    public CloudEventPubSub(
        ILogger<CloudEventPubSub> logger,
        IServiceProvider services,
        Registry2 registry,
        PubSubOptions options)
    {
        _options = options;
        _publishers = _options.Publishers;
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
        string pubsubName,
        string topic)
    {
        var metadata = new CloudEventMetadata(
            pubsubName,
            topic,
            cloudEvent.Type,
            cloudEvent.Source
        );
        return PublishAsync(cloudEvent, metadata);
    }

    private async Task<CloudEvent<TData>> PublishAsync<TData>(CloudEvent<TData> cloudEvent, CloudEventMetadata metadata)
    {
        using var activity = Tracing.CloudEventPublishing(metadata.PubSubName, metadata.Topic, cloudEvent);

        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent).ConfigureAwait(false);

        Logs.CloudEventPublished(_logger, metadata.PubSubName, metadata.Topic, metadata.Type, cloudEvent.Id);
        Metrics.CloudEventPublished(metadata);

        return cloudEvent;
    }

}
