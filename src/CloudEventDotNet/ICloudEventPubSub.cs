namespace CloudEventDotNet;

public interface ICloudEventPubSub
{
    Task PublishAsync<TData>(TData data);
}
