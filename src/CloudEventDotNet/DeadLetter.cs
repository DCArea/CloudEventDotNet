namespace CloudEventDotNet;

public record DeadLetter<TData>()
{
    public string PubSubName { get; set; } = default!;
    public string Topic { get; set; } = default!;
    public CloudEvent<TData> DeadEvent { get; set; } = default!;
    public DateTimeOffset DeadTime { get; set; } = default!;
    public string? DeadMessage { get; set; } = default!;
}


internal record DeadLetter(
    string PubSubName,
    string Topic,
    CloudEvent DeadEvent,
    DateTimeOffset DeadTime,
    string? DeadMessage
);
