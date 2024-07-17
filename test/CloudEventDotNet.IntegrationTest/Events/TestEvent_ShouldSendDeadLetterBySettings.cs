namespace CloudEventDotNet.IntegrationTest.Events;

[CloudEvent(DeadLetterTopic = "Test_DL_2", DeadLetterSource = "test.source2")]
public record TestEvent_ShouldSendDeadLetterBySettings
{
    public class Handler(SubscriptionMonitor<TestEvent_ShouldSendDeadLetterBySettings> monitor)
        : MonitoredHandler<TestEvent_ShouldSendDeadLetterBySettings>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEvent_ShouldSendDeadLetterBySettings> cloudEvent, CancellationToken token)
            => throw new NotImplementedException();
    }
}

[CloudEvent(Topic = "Test_DL_2", Source = "test.source2", Type = $"dl:{nameof(TestEvent_ShouldSendDeadLetterBySettings)}")]
public record TestEvent_ShouldSendDeadLetterBySettings_DeadLetter() : DeadLetter<TestEvent_ShouldSendDeadLetterBySettings>
{
    public class Handler(SubscriptionMonitor<TestEvent_ShouldSendDeadLetterBySettings_DeadLetter> monitor)
        : MonitoredHandler<TestEvent_ShouldSendDeadLetterBySettings_DeadLetter>(monitor)
    {
        public override Task HandleInternalAsync(CloudEvent<TestEvent_ShouldSendDeadLetterBySettings_DeadLetter> cloudEvent, CancellationToken token)
            => Task.CompletedTask;
    }
}
