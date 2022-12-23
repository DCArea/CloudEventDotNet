namespace CloudEventDotNet
{
    internal interface IDeadLetterSender
    {
        Task SendAsync(CloudEventMetadata metadata, CloudEvent cloudEvent, string deadMessage);
    }
}
