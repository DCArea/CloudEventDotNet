namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent]
public record Ping(string FA)
{
    public class Handler(SubscriptionMonitor<Ping> monitor)
        : MonitoredHandler<Ping>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<Ping> cloudEvent, CancellationToken token) => Task.CompletedTask;
    }
}
