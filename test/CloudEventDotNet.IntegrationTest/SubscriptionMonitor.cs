using System.Collections.Concurrent;
using System.Diagnostics;

namespace CloudEventDotNet.IntegrationTest;

public class SubscriptionMonitor<T>
{
    public ConcurrentBag<CloudEvent<T>> DeliveredEvents = new();

    [StackTraceHidden]
    [DebuggerHidden]
    public CloudEvent<T> WaitUntillDelivered(CloudEvent<T> cloudEvent, int timeoutSeconds = 5)
    {
        return WaitUntillDelivered(e => e.Id == cloudEvent.Id, timeoutSeconds);
    }

    [StackTraceHidden]
    [DebuggerHidden]
    public CloudEvent<T> WaitUntillDelivered(string type, int timeoutSeconds = 5)
    {
        return WaitUntillDelivered(e => e.Type == type, timeoutSeconds);
    }

    [StackTraceHidden]
    [DebuggerHidden]
    public CloudEvent<T> WaitUntillDelivered(Func<CloudEvent<T>, bool> predicate, int timeoutSeconds = 5)
    {
        WaitHelper.WaitUntill(() => DeliveredEvents.Any(predicate), timeoutSeconds);
        return DeliveredEvents.Single(predicate);
    }

    [StackTraceHidden]
    [DebuggerHidden]
    public void WaitUntillCount(int count, int timeoutSeconds = 5)
        => WaitHelper.WaitUntill(() => DeliveredEvents.Count == count, timeoutSeconds);
}
