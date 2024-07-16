namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent]
public record TestEventForRepublish2(string FA)
{
    public class Handler(SubscriptionMonitor<TestEventForRepublish2> monitor)
        : MonitoredHandler<TestEventForRepublish2>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish2> cloudEvent, CancellationToken token) => throw new NotImplementedException();
    }
};
