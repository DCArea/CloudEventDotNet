
namespace CloudEventDotNet;

/// <summary>
/// The handler of CloudEvent<typeparamref name="TData"/>
/// </summary>
/// <typeparam name="TData">The data type of the registered CloudEvent, it must has a <see cref="CloudEventAttribute"/> attribute</typeparam>
public interface ICloudEventHandler<TData>
{
    Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
