using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

internal interface ICloudEventHandlerFactory
{
    ICloudEventHandler Create(IServiceProvider services, CloudEventMetadata metadata, HandleCloudEventDelegate handlerDelegate);
}

internal class CloudEventHandlerFactory : ICloudEventHandlerFactory
{
    public ICloudEventHandler Create(IServiceProvider services, CloudEventMetadata metadata, HandleCloudEventDelegate handlerDelegate)
    {
        var handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, handlerDelegate);
        return handler;
    }
}
