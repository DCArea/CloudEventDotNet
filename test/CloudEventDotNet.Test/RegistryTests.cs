using CloudEventDotNet.TestEvents;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet.Test;

public class RegistryTests
{
    private readonly Registry _registry;

    public RegistryTests()
    {
        var services = new ServiceCollection()
            .AddLogging();
        services.AddCloudEvents()
            .Load(typeof(SimpleEvent).Assembly);
        var sp = services.BuildServiceProvider();

        _registry = sp.GetRequiredService<Registry>();
    }

    [Fact]
    public void Load()
    {
        var metadata = _registry.GetMetadata(typeof(SimpleEvent));
        Assert.True(_registry.TryGetHandler(metadata, out _));
    }

    [Fact]
    public void ShouldReturnDistinctTopics()
    {
        var topics = _registry.GetSubscribedTopics("default");

        Assert.Distinct(topics);
        Assert.DoesNotContain("NotInterest", topics);
    }

    [Fact]
    public void ShouldNotReturnNotSubscribedTopics()
    {
        var topics = _registry.GetSubscribedTopics("default");

        Assert.Distinct(topics);
        Assert.DoesNotContain("NotInterest", topics);
    }

}
