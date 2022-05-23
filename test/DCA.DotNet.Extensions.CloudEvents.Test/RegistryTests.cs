using DCA.DotNet.Extensions.CloudEvents.TestEvents;
using Microsoft.Extensions.DependencyInjection;

namespace DCA.DotNet.Extensions.CloudEvents.Test;

public class RegistryTests
{
    [Fact]
    public void Load()
    {
        var services = new ServiceCollection();
        services.AddCloudEvents()
            .Load(typeof(SimpleEvent).Assembly);
        var sp = services.BuildServiceProvider();

        var registry = sp.GetRequiredService<Registry>();
        var metadata = registry.GetMetadata(typeof(SimpleEvent));
        var handler = registry.GetHandler(metadata);
    }

}
