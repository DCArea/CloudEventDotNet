using System.Collections.Concurrent;
using System.Diagnostics;

namespace CloudEventDotNet.IntegrationTest;

public class SubscriptionMonitor<T>
{
    public ConcurrentBag<CloudEvent<T>> DeliveredEvents = new();

    [StackTraceHidden]
    [DebuggerHidden]
    public void WaitUntillDelivered<TData>(CloudEvent<TData> cloudEvent, int timeoutSeconds = 5)
        => WaitHelper.WaitUntill(() => DeliveredEvents.Any(e => e.Id == cloudEvent.Id), timeoutSeconds);

    [StackTraceHidden]
    [DebuggerHidden]
    public void WaitUntillCount(int count, int timeoutSeconds = 5)
        => WaitHelper.WaitUntill(() => DeliveredEvents.Count == count, timeoutSeconds);
}
