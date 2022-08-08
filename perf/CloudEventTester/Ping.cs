using CloudEventDotNet;

namespace CloudEventKafkaTester;

[CloudEvent]
public record Ping();

public sealed class PingHandler : ICloudEventHandler<Ping>
{
    private static long _count = 0;
    public static long Count => _count;
    public Task HandleAsync(CloudEvent<Ping> cloudEvent, CancellationToken token)
    {
        Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}
