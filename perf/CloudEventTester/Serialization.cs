using CloudEventDotNet;

namespace CloudEventKafkaTester;

[CloudEvent]
public record SerializationTest(string Foo);

public sealed class SerializationTestHandler : ICloudEventHandler<SerializationTest>
{
    public Task HandleAsync(CloudEvent<SerializationTest> cloudEvent, CancellationToken token)
    {
        if (cloudEvent.Data.Foo != "bar") throw new ArgumentException("data broken");
        return Task.CompletedTask;
    }
}
