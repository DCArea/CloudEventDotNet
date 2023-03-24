using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

public class PubSubDeadLetterSenderOptions
{
    public string? PubSubName { get; set; }
    public string? Topic { get; set; }
    public string? Source { get; set; }
}

internal class PubSubDeadLetterSender : IDeadLetterSender
{
    private readonly ILogger _logger;
    private readonly ICloudEventPubSub _pubsub;
    private readonly string _pubsubName;
    private readonly string _topic;
    private readonly string _source;

    public PubSubDeadLetterSender(
        ILogger<PubSubDeadLetterSender> logger,
        IOptions<PubSubOptions> pubSubOptions,
        ICloudEventPubSub pubsub,
        IOptions<PubSubDeadLetterSenderOptions> options)
    {
        _logger = logger;
        _pubsub = pubsub;
        var pubsubOptions = pubSubOptions.Value;
        _pubsubName = options.Value.PubSubName ?? pubsubOptions.DefaultPubSubName;
        _topic = options.Value.Topic ?? pubsubOptions.DefaultTopic;
        _source = options.Value.Source ?? pubsubOptions.DefaultSource;
    }

    public async Task SendAsync(CloudEventMetadata metadata, CloudEvent cloudEvent, string? deadMessage)
    {
        if (metadata.Type.StartsWith("dl:"))
        {
            return;
        }
        var deadLetter = new DeadLetter(metadata.PubSubName, metadata.Topic, cloudEvent, DateTimeOffset.UtcNow, deadMessage);
        var deadLetterCloudEvent = new CloudEvent<DeadLetter>(
            Id: "dl:" + cloudEvent.Id,
            Source: _source!,
            Type: "dl:" + cloudEvent.Type,
            Time: DateTimeOffset.UtcNow,
            deadLetter,
            null,
            null);
        await _pubsub.PublishAsync(deadLetterCloudEvent, _pubsubName, _topic);
        CloudEventPublishTelemetry.OnDeadLetterSent(_logger, deadLetterCloudEvent.Id, _pubsubName!, _topic!);
    }
}
