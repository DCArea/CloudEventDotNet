using System.Diagnostics;
using System.Text.Json;

namespace DCA.DotNet.Extensions.CloudEvents.Diagnostics;

internal static class Activities
{
    public static ActivitySource Source = new("DCA.CloudEvents");

    public static Activity? OnPublish<TData>(CloudEventMetadata metadata, CloudEvent<TData> cloudEvent)
    {
        var activity = Source.StartActivity($"CloudEvents Create  {cloudEvent.Type}", ActivityKind.Producer);
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
                cloudEvent.Extensions = new Dictionary<string, object?>{
                    { "traceparent", activity.Id },
                    { "tracestate", activity.TraceStateString },
                };
            }
        }
        return activity;
    }


    public static Activity? OnProcess(CloudEventMetadata metadata, CloudEvent cloudEvent)
    {
        ActivityContext.TryParse((string?)cloudEvent.Extensions?["traceparent"], (string?)cloudEvent.Extensions?["tracestate"], out var parentContext);
        var activity = Source.StartActivity($"CloudEvents Process  {cloudEvent.Type}", ActivityKind.Consumer, parentContext);
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
        }
        return activity;
    }

}
