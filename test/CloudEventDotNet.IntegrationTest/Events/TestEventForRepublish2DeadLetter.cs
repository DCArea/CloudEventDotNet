namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish2)}")]
public record TestEventForRepublish2DeadLetter() : DeadLetter<TestEventForRepublish2>
{
    public class Handler(SubscriptionMonitor<TestEventForRepublish2DeadLetter> monitor)
        : MonitoredHandler<TestEventForRepublish2DeadLetter>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish2DeadLetter> cloudEvent, CancellationToken token) => throw new NotImplementedException();
    }
}
