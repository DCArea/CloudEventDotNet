using CloudEventDotNet.Telemetry;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal class PubSubDeadLetterSender(
    ILogger<PubSubDeadLetterSender> logger,
    ICloudEventPubSub pubsub) : IDeadLetterSender
{
    private readonly ILogger _logger = logger;

    public async Task SendAsync(CloudEventMetadata metadata, CloudEvent cloudEvent, string? deadMessage, DeadLetterOptions options)
    {
        if (metadata.Type.StartsWith("dl:"))
        {
            return;
        }
        var deadLetter = new DeadLetter(metadata.PubSubName, metadata.Topic, cloudEvent, DateTimeOffset.UtcNow, deadMessage);
        var deadLetterCloudEvent = new CloudEvent<DeadLetter>(
            Id: "dl:" + cloudEvent.Id,
            Source: options.Source,
            Type: "dl:" + cloudEvent.Type,
            Time: DateTimeOffset.UtcNow,
            deadLetter,
            null,
            null);
        await pubsub.PublishAsync(deadLetterCloudEvent, options.PubSubName, options.Topic);
        Logs.DeadLetterSent(_logger, options.PubSubName, options.Topic, deadLetterCloudEvent.Type, deadLetterCloudEvent.Id);
    }
}
