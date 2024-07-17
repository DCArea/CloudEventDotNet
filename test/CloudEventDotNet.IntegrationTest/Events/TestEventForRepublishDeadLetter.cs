namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish)}")]
public record TestEventForRepublishDeadLetter() : DeadLetter<TestEventForRepublish>
{
    public class Handler(SubscriptionMonitor<TestEventForRepublishDeadLetter> monitor)
        : MonitoredHandler<TestEventForRepublishDeadLetter>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEventForRepublishDeadLetter> cloudEvent, CancellationToken token)
        {
            //cloudEvent.Data.DeadEvent.Should().NotBeNull();
            return Task.CompletedTask;
        }
    }
}
