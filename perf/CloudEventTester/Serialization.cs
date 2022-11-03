using CloudEventDotNet;

namespace CloudEventTester;

[CloudEvent]
public record SerializationTest(string Foo);

public sealed class SerializationTestHandler : ICloudEventHandler<SerializationTest>
{
    public Task HandleAsync(CloudEvent<SerializationTest> cloudEvent, CancellationToken token)
            => cloudEvent.Data.Foo != "bar" ? throw new ArgumentException("data broken") : Task.CompletedTask;
}
