using DCA.DotNet.Extensions.CloudEvents;

namespace Microsoft.Extensions.DependencyInjection;


public static class CloudEventsServiceCollectionExtensions
{
    public static PubSubBuilder AddCloudEvents(
        this IServiceCollection services,
        string defaultPubSubName = "default",
        string defaultTopic = "default",
        string defaultSource = "default")
    {
        return new PubSubBuilder(services, defaultPubSubName, defaultTopic, defaultSource);
    }
}
