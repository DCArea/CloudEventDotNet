using System.Text.Json;
using CloudEventDotNet.IntegrationTest.Events;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

public class PublishTests : RedisPubSubTestBase
{
    [Fact]
    public async Task PublishCloudEvent()
    {
        var ping = new Ping(Guid.NewGuid().ToString());
        await PubSub.PublishAsync(ping);

        Assert.NotEmpty(PublishedCloudEvents);
        var published = PublishedCloudEvents.Single();
        published.Data.Deserialize<Ping>(JSON.DefaultJsonSerializerOptions).Should().Be(ping);
    }
}
