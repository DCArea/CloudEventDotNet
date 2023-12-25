using Microsoft.Extensions.DependencyInjection;
using Polly.Registry;

namespace CloudEventDotNet;

internal interface ICloudEventHandlerFactory
{
    ICloudEventHandler Create(IServiceProvider services, CloudEventMetadata metadata, HandleCloudEventDelegate handlerDelegate);
}

internal class CloudEventHandlerFactory(
    ResiliencePipelineProvider<string> rpp) : ICloudEventHandlerFactory
{
    public const string ResiliencePolicyName = nameof(CloudEventHandler);

    public ICloudEventHandler Create(IServiceProvider services, CloudEventMetadata metadata, HandleCloudEventDelegate handlerDelegate)
    {
        CloudEventHandler handler;
        if (rpp.TryGetPipeline(nameof(CloudEventHandler), out var pipeline))
        {
            handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, pipeline, metadata, handlerDelegate);
        }
        else
        {
            handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, handlerDelegate);
        }
        return handler;
    }
}
