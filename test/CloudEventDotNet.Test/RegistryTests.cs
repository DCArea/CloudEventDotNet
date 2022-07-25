using CloudEventDotNet.TestEvents;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet.Test;

public class RegistryTests
{
    [Fact]
    public void Load()
    {
        var services = new ServiceCollection()
            .AddLogging();
        services.AddCloudEvents()
            .Load(typeof(SimpleEvent).Assembly);
        var sp = services.BuildServiceProvider();

        var registry = sp.GetRequiredService<Registry>();
        var metadata = registry.GetMetadata(typeof(SimpleEvent));
        Assert.True(registry.TryGetHandler(metadata, out var handler));
    }

}
