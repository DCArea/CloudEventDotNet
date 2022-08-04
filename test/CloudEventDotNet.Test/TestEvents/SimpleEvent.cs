namespace CloudEventDotNet.TestEvents;

[CloudEvent]
public record SimpleEvent(
    string Foo,
    string Bar
);

[CloudEvent]
public record SimpleEvent2(
    string Foo,
    string Bar
);

[CloudEvent(Topic = "NotInterest")]
public record SimpleEvent3(
    string Foo,
    string Bar
);

public sealed class SimpleEventHandler : ICloudEventHandler<SimpleEvent>
{
    public Task HandleAsync(CloudEvent<SimpleEvent> cloudEvent, CancellationToken token)
    {
        return Task.CompletedTask;
    }
}
