namespace CloudEventDotNet;

internal record class SubscriptionOptions(
    DeadLetterOptions? DeadLetter)
{
    public static SubscriptionOptions Create(
        Type eventDataType,
        bool enableDeadLetter,
        string? deadLetterPubSubName,
        string? deadLetterSource,
        string? deadLetterTopic
        )
    {
        DeadLetterOptions? deadLetterOptions = null;
        if (enableDeadLetter)
        {
            if (string.IsNullOrEmpty(deadLetterPubSubName))
            {
                ThrowHelper.ConfigMissing(eventDataType, nameof(deadLetterPubSubName));
            }
            if (string.IsNullOrEmpty(deadLetterSource))
            {
                ThrowHelper.ConfigMissing(eventDataType, nameof(deadLetterSource));
            }
            if (string.IsNullOrEmpty(deadLetterTopic))
            {
                ThrowHelper.ConfigMissing(eventDataType, nameof(deadLetterTopic));
            }
            deadLetterOptions = new DeadLetterOptions(deadLetterPubSubName, deadLetterSource, deadLetterTopic);
        }
        return new SubscriptionOptions(deadLetterOptions);
    }
};
