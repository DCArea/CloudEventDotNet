namespace CloudEventDotNet.IntegrationTest;

public abstract class MonitoredHandler<TData>(SubscriptionMonitor<TData> monitor) : ICloudEventHandler<TData>
{
    public Task HandleAsync(CloudEvent<TData> cloudEvent, CancellationToken token)
    {
        monitor.DeliveredEvents.Add(cloudEvent);
        return HandleInternalAsync(cloudEvent, token);
    }

    public abstract Task HandleInternalAsync(CloudEvent<TData> cloudEvent, CancellationToken token);
}
