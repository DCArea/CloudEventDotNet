﻿using System.Text.Json;
using CloudEventDotNet.IntegrationTest.Events;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

public class ProcessTests : RedisPubSubTestBase
{
    [Fact]
    public async Task Subscribe()
    {
        await Subscriber.StartAsync(default);

        var ping = new Ping(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(ping);
        var monitor = GetMonitor<Ping>();
        monitor.WaitUntillDelivered(pe, 3);

        await StopAsync();

        monitor.DeliveredEvents.Count.Should().Be(1);
    }

    [Fact]
    public async Task ShouldSendDeadLetter()
    {
        await Subscriber.StartAsync(default);

        var e = new TestEventForRepublish(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(e);
        WaitHelper.WaitUntill(() => PublishedCloudEvents.Count == 2);
        GetMonitor<TestEventForRepublishDeadLetter>().WaitUntillCount(1);

        await StopAsync();

        var de = PublishedCloudEvents.Single(e => e.Type.StartsWith("dl:"));
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish)}");
        var deadLetter = de.Data.Deserialize<TestEventForRepublishDeadLetter>(JSON.DefaultJsonSerializerOptions)!;
        deadLetter.DeadEvent.Should().BeEquivalentTo(pe);
    }

    [Fact]
    public async Task ShouldNotSendDeadLetterForDeadLetter()
    {
        await Subscriber.StartAsync(default);

        var ping = new TestEventForRepublish2(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(ping);
        GetMonitor<TestEventForRepublish2>().WaitUntillDelivered(pe);
        GetMonitor<TestEventForRepublish2DeadLetter>().WaitUntillCount(1);
        await StopAsync();

        PublishedCloudEvents.Should().HaveCount(2);
        var de = PublishedCloudEvents.Single(e => e.Type.StartsWith("dl:"));
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish2)}");
        var deadLetter = de.Data.Deserialize<TestEventForRepublish2DeadLetter>(JSON.DefaultJsonSerializerOptions)!;
        deadLetter.DeadEvent.Should().BeEquivalentTo(pe);
    }

    [Fact]
    public async Task DequeueTest()
    {
        await Subscriber.StartAsync(default);
        for (int i = 0; i < 100; i++)
        {
            var ping = new Ping(Guid.NewGuid().ToString());
            await Pubsub.PublishAsync(ping);
        }
        GetMonitor<Ping>().WaitUntillCount(100, 10);
        await StopAsync();
    }
}
