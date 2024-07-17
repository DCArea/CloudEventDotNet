using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace CloudEventDotNet;

internal class Registry2(
    FrozenDictionary<Type, CloudEventMetadata> metadata,
    //FrozenDictionary<CloudEventMetadata, ICloudEventHandler> handlers,
    FrozenDictionary<CloudEventMetadata, CloudEventSubscription> subscriptions
    )
{
    //internal readonly FrozenDictionary<Type, CloudEventMetadata> _metadata = metadata;
    //internal readonly FrozenDictionary<CloudEventMetadata, ICloudEventHandler> _handlers = handlers;

    internal CloudEventMetadata GetMetadata(Type eventDataType)
        => metadata[eventDataType];

    internal bool TryGetSubscription(CloudEventMetadata metadata, [NotNullWhen(true)] out CloudEventSubscription? sub)
        => subscriptions.TryGetValue(metadata, out sub);

    /// <summary>
    /// Get topics subscribed by specified pubsub
    /// </summary>
    /// <param name="pubSubName">The pubsub name</param>
    /// <returns></returns>
    public IEnumerable<string> GetSubscribedTopics(string pubSubName)
        => subscriptions.Keys
            .Where(m => m.PubSubName == pubSubName)
            .Select(m => m.Topic)
            .Distinct();

    /// <summary>
    /// Show registered metadata and handlers
    /// </summary>
    /// <returns>Registered metadata and handlers</returns>
    public string Debug()
    {
        var sb = new StringBuilder();

        sb.AppendLine("Metadata:");
        foreach (var (key, value) in metadata)
        {
            sb.AppendLine($"{key}: {value}");
        }

        sb.AppendLine("Handlers:");
        foreach (var (key, value) in subscriptions)
        {
            sb.AppendLine($"{key}: {value.Handler}");
        }

        return sb.ToString();
    }

}
