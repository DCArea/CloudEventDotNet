using System.ComponentModel.DataAnnotations;

namespace CloudEventDotNet;

public class PubSubOptions
{
    internal Dictionary<string, Func<IServiceProvider, ICloudEventPublisher>> PublisherFactoris { get; set; } = new();
    internal Dictionary<string, Func<IServiceProvider, ICloudEventSubscriber>> SubscriberFactoris { get; set; } = new();
}
