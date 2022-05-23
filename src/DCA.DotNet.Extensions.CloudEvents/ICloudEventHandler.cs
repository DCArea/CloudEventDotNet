
namespace DCA.DotNet.Extensions.CloudEvents;

public interface ICloudEventHandler<TData>
{
    Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
