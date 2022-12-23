namespace CloudEventDotNet;

public class PubSubOptions
{
    public string DefaultPubSubName { get; set; } = default!;
    public string DefaultTopic { get; set; } = default!;
    public string DefaultSource { get; set; } = default!;

    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new();
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
