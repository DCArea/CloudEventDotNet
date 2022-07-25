
namespace CloudEventDotNet;

public interface ICloudEventHandler<TData>
{
    Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
