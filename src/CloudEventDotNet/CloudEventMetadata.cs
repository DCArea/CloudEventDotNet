namespace CloudEventDotNet;

/// <summary>
/// CloudEvents metadata, used to distinguish between different CloudEvents
/// </summary>
/// <param name="PubSubName"></param>
/// <param name="Topic"></param>
/// <param name="Type"></param>
/// <param name="Source"></param>
public record struct CloudEventMetadata(
    string PubSubName,
    string Topic,
    string Type,
    string Source
)
{
    public static CloudEventMetadata Create(
        Type eventDataType,
        string? pubSubName,
        string? topic,
        string? type,
        string? source
        )
    {
        return new CloudEventMetadata(
            pubSubName ?? ThrowHelper.ConfigMissing<string>(eventDataType, nameof(pubSubName)),
            topic ?? ThrowHelper.ConfigMissing<string>(eventDataType, nameof(topic)),
            type ?? ThrowHelper.ConfigMissing<string>(eventDataType, nameof(type)),
            source ?? ThrowHelper.ConfigMissing<string>(eventDataType, nameof(source))
            );
    }

}
