using CloudEventDotNet;

namespace CloudEventTester;

[CloudEvent]
public record DLTest();

public class DLTestHandler : ICloudEventHandler<DLTest>
{
    private static long s_count = 0;
    public static long Count => s_count;
    public Task HandleAsync(CloudEvent<DLTest> cloudEvent, CancellationToken token)
    {
        _ = Interlocked.Increment(ref s_count);
        throw new NotImplementedException();
    }
}

[CloudEvent(Topic = "DL", Type = $"dl:{nameof(DLTest)}")]
public record DLTestDeadLetter() : DeadLetter<DLTest>();

public class DLTestDeadLetterHandler : ICloudEventHandler<DLTestDeadLetter>
{
    private static long s_count = 0;
    public static long Count => s_count;
    public Task HandleAsync(CloudEvent<DLTestDeadLetter> cloudEvent, CancellationToken token)
    {
        _ = Interlocked.Increment(ref s_count);
        return Task.CompletedTask;
    }
}
