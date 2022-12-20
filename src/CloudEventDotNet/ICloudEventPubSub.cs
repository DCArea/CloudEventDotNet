namespace CloudEventDotNet;

/// <summary>
/// Represents a publisher of CloudEvents.
/// </summary>
public interface ICloudEventPubSub
{
    /// <summary>
    /// Publish a CloudEvent with registered metadata.
    /// </summary>
    /// <typeparam name="TData">The data type of the CloudEvent, must registered with <see cref="CloudEventAttribute"/></typeparam>
    /// <param name="data">The data of the CloudEvent</param>
    /// <returns>The <see cref="Task"/> that represents the asynchronous operation, carries the published CloudEvent.</returns>
    Task<CloudEvent<TData>> PublishAsync<TData>(TData data);
}
