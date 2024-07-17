using CloudEventDotNet;

namespace Microsoft.Extensions.DependencyInjection;


/// <summary>
/// Extensions to scan for CloudEvents and configure them.
/// </summary>
public static class CloudEventsServiceCollectionExtensions
{

    /// <summary>
    /// Register CloudEvents metadata and handlers
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="defaultPubSubName">The default PubSub name</param>
    /// <param name="defaultTopic">The default topic</param>
    /// <param name="defaultSource">The default source</param>
    public static PubSubBuilder AddCloudEvents(
        this IServiceCollection services,
        string? defaultPubSubName = null,
        string? defaultTopic = null,
        string? defaultSource = null)
    {
        return new PubSubBuilder(services, defaultPubSubName, defaultTopic, defaultSource);
    }
}
