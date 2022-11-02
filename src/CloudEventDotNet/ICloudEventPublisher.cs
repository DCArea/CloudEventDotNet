namespace CloudEventDotNet;

public interface ICloudEventPublisher
{
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}

internal interface ICloudEventRepublisher
{
    Task RepublishAsync(string topic, CloudEvent cloudEvent);
}
