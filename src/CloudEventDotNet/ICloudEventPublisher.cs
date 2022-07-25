namespace CloudEventDotNet;

public interface ICloudEventPublisher
{
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}
