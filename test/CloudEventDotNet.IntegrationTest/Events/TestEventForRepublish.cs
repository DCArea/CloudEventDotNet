namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent]
public record TestEventForRepublish(string FA)
{
    public class Handler(SubscriptionMonitor<TestEventForRepublish> monitor)
        : MonitoredHandler<TestEventForRepublish>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish> cloudEvent, CancellationToken token) => throw new NotImplementedException();
    }
};
