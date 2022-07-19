using System.Diagnostics;
using System.Text.Json;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics;
using DCA.DotNet.Extensions.CloudEvents.TestEvents;

namespace DCA.DotNet.Extensions.CloudEvents.Test.Diagnostics;
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
        var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(JsonSerializer.Serialize(sourceEvent));
        var metadata = new CloudEventMetadata("testpubsub", "testtopic", cloudEvent!.Type, cloudEvent.Source);

        var publishActivity = Activities.OnPublish(metadata, sourceEvent);
        Assert.NotNull(publishActivity);
        publishActivity!.Stop();
        Activity.Current = null;

        var processActivity = Activities.OnProcess(metadata, cloudEvent);
        Assert.NotNull(processActivity);
    }
}
