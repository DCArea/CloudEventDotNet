namespace DCA.DotNet.Extensions.CloudEvents;

public interface ICloudEventSubscriber
{
    Task Subscribe(CancellationToken token);
}
