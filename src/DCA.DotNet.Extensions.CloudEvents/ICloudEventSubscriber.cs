namespace DCA.DotNet.Extensions.CloudEvents;

public interface ICloudEventSubscriber
{
    Task StartAsync();
    Task StopAsync();
}
