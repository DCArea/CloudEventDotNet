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
    /// <param name="id">The id of the CloudEvent, default to <code>Guid.NewGuid().ToString()</code>. </param>
    /// <param name="time">The time of the CloudEvent, default to <code>DateTimeOffset.UtcNow</code>. </param>
    /// <param name="subject">The subject of the CloudEvent, default to <see langword="null"/>. </param>
    /// <returns>The <see cref="Task"/> that represents the asynchronous operation, carries the published CloudEvent.</returns>
    Task<CloudEvent<TData>> PublishAsync<TData>(TData data,
        string? id = null,
        DateTimeOffset? time = null,
        string? subject = null);

    /// <summary>
    /// Publish a CloudEvent.
    /// </summary>
    /// <typeparam name="TData">The data type of the CloudEvent. </typeparam>
    /// <param name="cloudEvent">The prepared CloudEvent</param>
    /// <param name="pubsubName">The pubsub name for publishing</param>
    /// <param name="topic">The destination topic</param>
    /// <returns></returns>
    public Task<CloudEvent<TData>> PublishAsync<TData>(
        CloudEvent<TData> cloudEvent,
        string pubsubName,
        string topic);

    ///// <summary>
    ///// Publish a CloudEvent.
    ///// </summary>
    ///// <typeparam name="TData">The data type of the CloudEvent, must registered with <see cref="CloudEventAttribute"/></typeparam>
    ///// <param name="data">The data of the CloudEvent</param>
    ///// <param name="metadata">The metadata of the CloudEvent</param>
    ///// <returns>The <see cref="Task"/> that represents the asynchronous operation, carries the published CloudEvent.</returns>
    //Task<CloudEvent<TData>> PublishAsync<TData>(TData data, CloudEventMetadata metadata);
}
