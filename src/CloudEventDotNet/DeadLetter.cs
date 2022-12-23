using System.Text.Json.Serialization;

namespace CloudEventDotNet;

public record DeadLetter<TData>()
{
    //[JsonPropertyName("pubSubName")]
    public string PubSubName { get; set; } = default!;
    //[JsonPropertyName("topic")]
    public string Topic { get; set; } = default!;
    //[JsonPropertyName("deadEvent")]
    public CloudEvent<TData> DeadEvent { get; set; } = default!;
    //[JsonPropertyName("deadTime")]
    public DateTimeOffset DeadTime { get; set; } = default!;
    //[JsonPropertyName("deadMessage")]
    public string? DeadMessage { get; set; } = default!;
}


internal record DeadLetter(
    string PubSubName,
    string Topic,
    CloudEvent DeadEvent,
    DateTimeOffset DeadTime,
    string? DeadMessage
);
