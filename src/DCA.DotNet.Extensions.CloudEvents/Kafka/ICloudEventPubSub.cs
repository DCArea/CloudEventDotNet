namespace DCA.DotNet.Extensions.CloudEvents;

public interface ICloudEventPubSub
{
    Task PublishAsync<TData>(TData data);
}
