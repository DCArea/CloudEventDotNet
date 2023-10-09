using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CloudEventDotNet.Telemetry;

internal static class Tracing
{
    internal static ActivitySource Source { get; } = new("DCA.CloudEvents");

    public static Activity? CloudEventPublishing<TData>(string pubsub, string topic, CloudEvent<TData> cloudEvent)
    {
        var activity = Source.StartActivity($"CloudEvents Create {cloudEvent.Type}", ActivityKind.Producer);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", pubsub);
            activity.SetTag("messaging.destination", topic);
            activity.SetTag("messaging.destination_kind", "topic");

            activity.SetTag("cloudevents.event_id", cloudEvent.Id.ToString());
            activity.SetTag("cloudevents.event_source", cloudEvent.Source);
            activity.SetTag("cloudevents.event_type", cloudEvent.Type);
            if (cloudEvent.Subject is not null)
            {
                activity.SetTag("cloudevents.event_subject", cloudEvent.Subject);
            }
            cloudEvent.Extensions ??= [];
            cloudEvent.Extensions["traceparent"] = JsonValue.Create(activity.Id).Deserialize<JsonElement>();
            cloudEvent.Extensions["tracestate"] = JsonValue.Create(activity.TraceStateString).Deserialize<JsonElement>();
        }
        return activity;
    }

    public static Activity? OnProcessing(string pubsub, string topic, CloudEvent cloudEvent)
    {
        ActivityContext parentContext = default;
        if (cloudEvent.Extensions is not null && cloudEvent.Extensions.Count != 0)
        {
            cloudEvent.Extensions.TryGetValue("traceparent", out var traceparent);
            cloudEvent.Extensions.TryGetValue("tracestate", out var tracestate);

            ActivityContext.TryParse(traceparent.GetString(), tracestate.GetString(), out parentContext);
        }
        var activity = Source.StartActivity($"CloudEvents Process {cloudEvent.Type}", ActivityKind.Consumer, parentContext);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", pubsub);
            activity.SetTag("messaging.destination", topic);
            activity.SetTag("messaging.destination_kind", "topic");

            activity.SetTag("cloudevents.event_id", cloudEvent.Id);
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
