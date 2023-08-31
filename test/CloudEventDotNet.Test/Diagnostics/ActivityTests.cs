using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using CloudEventDotNet.Telemetry;
using CloudEventDotNet.TestEvents;

namespace CloudEventDotNet.Test.Diagnostics;
public partial class ActivityTests
{
    private static readonly ActivityListener s_listener;

    static ActivityTests()
    {
        s_listener = new()
        {
            ShouldListenTo = p => p.Name == "DCA.CloudEvents",
            Sample = Sample,
            SampleUsingParentId = SampleUsingParentId,
        };

        static ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> options)
        {
            return ActivitySamplingResult.AllDataAndRecorded;
        };

        static ActivitySamplingResult SampleUsingParentId(ref ActivityCreationOptions<string> options)
        {
            return ActivitySamplingResult.AllDataAndRecorded;
        };
    }

    public ActivityTests()
    {
        ActivitySource.AddActivityListener(s_listener);
    }

    [Fact]
    public void RoundTrip()
    {
        var sourceEvent = new CloudEvent<SimpleEvent>(
            Id: Guid.NewGuid().ToString(),
            Source: "testsource",
            Type: "simpleevent",
            DateTimeOffset.UtcNow,
            Data: new SimpleEvent("foo", "bar"),
            null,
            null
        );
        var metadata = new CloudEventMetadata("testpubsub", "testtopic", sourceEvent!.Type, sourceEvent.Source);

        var publishActivity = Tracing.CloudEventPublishing(metadata.PubSubName, metadata.Topic, sourceEvent);
        Assert.NotNull(publishActivity);
        Assert.Equal(publishActivity!.Id?.ToString(), sourceEvent.Extensions["traceparent"].GetString());
        Assert.Equal(publishActivity.TraceStateString?.ToString(), sourceEvent.Extensions["tracestate"].GetString());
        publishActivity.Stop();
        Activity.Current = null;


        var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(JsonSerializer.Serialize(sourceEvent));
        metadata = new CloudEventMetadata("testpubsub", "testtopic", cloudEvent!.Type, cloudEvent.Source);
        Assert.Equal(publishActivity!.Id?.ToString(), cloudEvent!.Extensions["traceparent"].GetString());
        Assert.Equal(publishActivity.TraceStateString?.ToString(), cloudEvent.Extensions["tracestate"].GetString());

        var processActivity = Tracing.OnProcessing(metadata.PubSubName, metadata.Topic, cloudEvent);
        Assert.NotNull(processActivity);

        Assert.Equal(publishActivity!.Id?.ToString(), processActivity!.ParentId);
    }


    [Fact]
    public void ShouldIgnoreNullTraceContext()
    {
        var sourceEvent = new CloudEvent<SimpleEvent>(
            Id: Guid.NewGuid().ToString(),
            Source: "testsource",
            Type: "simpleevent",
            DateTimeOffset.UtcNow,
            Data: new SimpleEvent("foo", "bar"),
            null,
            null
        );
        sourceEvent.Extensions["traceparent"] = JsonSerializer.Deserialize<JsonElement>((JsonValue?)null);
        sourceEvent.Extensions["tracestate"] = JsonSerializer.Deserialize<JsonElement>((JsonValue?)null);

        var json = JsonSerializer.Serialize(sourceEvent);
        var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(json);
        var metadata = new CloudEventMetadata("testpubsub", "testtopic", cloudEvent!.Type, cloudEvent.Source);
        var processActivity = Tracing.OnProcessing(metadata.PubSubName, metadata.Topic, cloudEvent);
        Assert.NotNull(processActivity);
    }
}
