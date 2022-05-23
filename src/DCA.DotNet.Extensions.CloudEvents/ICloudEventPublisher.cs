namespace DCA.DotNet.Extensions.CloudEvents;

public interface ICloudEventPublisher
{
    Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent);
}
