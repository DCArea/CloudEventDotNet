﻿using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

public class PubSubDeadLetterSenderOptions
{
    public string? PubSubName { get; set; }
    public string? Topic { get; set; }
    public string? Source { get; set; }
}

internal class PubSubDeadLetterSender : IDeadLetterSender
{
    private readonly ICloudEventPubSub _pubsub;
    private readonly PubSubDeadLetterSenderOptions _options;

    public PubSubDeadLetterSender(IOptions<PubSubOptions> pubSubOptions, ICloudEventPubSub pubsub, IOptions<PubSubDeadLetterSenderOptions> options)
    {
        var _registry = pubSubOptions.Value;
        _pubsub = pubsub;
        _options = options.Value;
        _options.PubSubName ??= _registry.DefaultPubSubName;
        _options.Topic ??= _registry.DefaultTopic;
        _options.Source ??= _registry.DefaultSource;
    }

    public async Task SendAsync(CloudEventMetadata metadata, CloudEvent cloudEvent, string? deadMessage)
    {
        if (metadata.Type.StartsWith("dl:"))
        {
            return;
        }
        var deadEventMetadata = new CloudEventMetadata(_options.PubSubName!, _options.Topic!, $"dl:{metadata.Type}", _options.Source!);
        var deadLetter = new DeadLetter(metadata.PubSubName, metadata.Topic, cloudEvent, DateTimeOffset.UtcNow, deadMessage);
        await _pubsub.PublishAsync(deadLetter, deadEventMetadata);
    }
}