using System.Collections.Frozen;
using System.Reflection;

namespace CloudEventDotNet;

public class PubSubOptionsBuilder
{
    public List<Assembly> Assemblies { get; set; } = [];

    public string? DefaultPubSubName { get; set; }
    public string? DefaultTopic { get; set; }
    public string? DefaultSource { get; set; }

    public bool EnableDeadLetter { get; set; }
    public string? DefaultDeadLetterPubSubName { get; set; }
    public string? DefaultDeadLetterSource { get; set; }
    public string? DefaultDeadLetterTopic { get; set; }

    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactories { get; set; } = [];
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactories { get; set; } = [];

    public PubSubOptions Build(IServiceProvider sp)
    {
        var publishers = PublisherFactories.ToFrozenDictionary(kvp => kvp.Key, kvp => kvp.Value(sp));
        var subscribers = SubscriberFactories.ToFrozenDictionary(kvp => kvp.Key, kvp => kvp.Value(sp));
        return new PubSubOptions(
            publishers,
            subscribers
            );
    }
}

public record class PubSubOptions(
    FrozenDictionary<string, ICloudEventPublisher> Publishers,
    FrozenDictionary<string, ICloudEventSubscriber> Subscribers
    );

