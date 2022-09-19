namespace CloudEventDotNet;

/// <summary>
/// CloudEvents metadata.
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
);
