using System.ComponentModel.DataAnnotations;

namespace CloudEventDotNet;

public class PubSubOptions
{
    public Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new();
    public Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
