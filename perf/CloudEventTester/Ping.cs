using CloudEventDotNet;

namespace CloudEventKafkaTester;

[CloudEvent]
public record Ping();

public class PingHandler : ICloudEventHandler<Ping>
{
    private static long s_count = 0;
    public static long Count => s_count;
    public Task HandleAsync(CloudEvent<Ping> cloudEvent, CancellationToken token)
    {
        Interlocked.Increment(ref s_count);
        return Task.CompletedTask;
    }
}
