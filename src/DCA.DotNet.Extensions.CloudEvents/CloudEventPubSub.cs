using DCA.DotNet.Extensions.CloudEvents.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DCA.DotNet.Extensions.CloudEvents;

internal class CloudEventPubSub : ICloudEventPubSub
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

    public async Task PublishAsync<TData>(TData data)
    {
        var dataType = typeof(TData);
        var metadata = _registry.GetMetadata(dataType);
        var cloudEvent = new CloudEvent<TData>(
            Id: Guid.NewGuid().ToString(),
            Source: metadata.Source,
            Type: metadata.Type,
            Time: DateTimeOffset.UtcNow,
            Data: data,
            DataSchema: null,
            Subject: null
        );
        using var activity = Activities.OnPublish(metadata, cloudEvent);
        var publisher = _publishers[metadata.PubSubName];
        await publisher.PublishAsync(metadata.Topic, cloudEvent);
        Metrics.OnCloudEventPublished(metadata);
    }
}
