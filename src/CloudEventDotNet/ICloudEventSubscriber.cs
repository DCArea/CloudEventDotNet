namespace CloudEventDotNet;

public interface ICloudEventSubscriber
{
    Task StartAsync();
    Task StopAsync();
}
