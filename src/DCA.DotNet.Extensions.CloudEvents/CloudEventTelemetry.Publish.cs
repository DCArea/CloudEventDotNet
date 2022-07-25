
using System.Diagnostics;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;

namespace DCA.DotNet.Extensions.CloudEvents;

internal static class CloudEventPublishTelemetry
{
    static CloudEventPublishTelemetry()
    {
        s_cloudEventsPublished = new(CloudEventTelemetry.Meter, "dca_cloudevents_published");
    }

    public static Activity? OnCloudEventPublishing<TData>(CloudEventMetadata metadata, CloudEvent<TData> cloudEvent)
    {
        var activity = CloudEventTelemetry.Source.StartActivity($"CloudEvents Create {cloudEvent.Type}", ActivityKind.Producer);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", metadata.PubSubName);
            activity.SetTag("messaging.destination", metadata.Topic);
            activity.SetTag("messaging.destination_kind", "topic");

            activity.SetTag("cloudevents.event_id", cloudEvent.Id.ToString());
            activity.SetTag("cloudevents.event_source", cloudEvent.Source);
            activity.SetTag("cloudevents.event_type", cloudEvent.Type);
            if (cloudEvent.Subject is not null)
            {
                activity.SetTag("cloudevents.event_subject", cloudEvent.Subject);
            }
            if (cloudEvent.Extensions is null)
            {
                cloudEvent.Extensions = new();
            }
            cloudEvent.Extensions["traceparent"] = activity.Id;
            cloudEvent.Extensions["tracestate"] = activity.TraceStateString;
        }
        return activity;
    }

    private static readonly CounterAggregatorGroup s_cloudEventsPublished;
    public static void OnCloudEventPublished(CloudEventMetadata metadata)
    {
        var aggregator = s_cloudEventsPublished.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        aggregator.Add(1);
    }

}
