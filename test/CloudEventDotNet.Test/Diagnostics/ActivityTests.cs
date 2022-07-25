using System.Diagnostics;
using System.Text.Json;
using CloudEventDotNet.Diagnostics;
using CloudEventDotNet.TestEvents;
using Microsoft.Extensions.Logging.Abstractions;

namespace CloudEventDotNet.Test.Diagnostics;
public class ActivityTests
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

        var publishActivity = CloudEventPublishTelemetry.OnCloudEventPublishing(metadata, sourceEvent);
        Assert.NotNull(publishActivity);
        Assert.Equal(publishActivity!.Id?.ToString(), sourceEvent.Extensions["traceparent"]?.ToString());
        Assert.Equal(publishActivity.TraceStateString?.ToString(), sourceEvent.Extensions["tracestate"]?.ToString());
        publishActivity.Stop();
        Activity.Current = null;


        var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(JsonSerializer.Serialize(sourceEvent));
        metadata = new CloudEventMetadata("testpubsub", "testtopic", cloudEvent!.Type, cloudEvent.Source);
        Assert.Equal(publishActivity!.Id?.ToString(), cloudEvent!.Extensions["traceparent"].GetString());
        Assert.Equal(publishActivity.TraceStateString?.ToString(), cloudEvent.Extensions["tracestate"].GetString());

        var processTelemetry = new CloudEventProcessingTelemetry(NullLoggerFactory.Instance, metadata);
        var processActivity = processTelemetry.OnProcessing(cloudEvent);
        Assert.NotNull(processActivity);

        Assert.Equal(publishActivity!.Id?.ToString(), processActivity!.ParentId);
    }


    [Fact]
    public void ShouldIgnoreNullTraceContext()
    {
        // var traceParentId = new Activity("test").Id;
        var sourceEvent = new CloudEvent<SimpleEvent>(
            Id: Guid.NewGuid().ToString(),
            Source: "testsource",
            Type: "simpleevent",
            DateTimeOffset.UtcNow,
            Data: new SimpleEvent("foo", "bar"),
            null,
            null
        );
        sourceEvent.Extensions["traceparent"] = null;
        sourceEvent.Extensions["tracestate"] = null;

        var json = JsonSerializer.Serialize(sourceEvent);
        var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(json);
        var metadata = new CloudEventMetadata("testpubsub", "testtopic", cloudEvent!.Type, cloudEvent.Source);
        var processTelemetry = new CloudEventProcessingTelemetry(NullLoggerFactory.Instance, metadata);
        var processActivity = processTelemetry.OnProcessing(cloudEvent);
        Assert.NotNull(processActivity);
    }
}
