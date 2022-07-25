using System.Diagnostics.Metrics;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal static class KafkaTelemetry
{
    public static Meter Meter { get; }
    static KafkaTelemetry()
    {
        Meter = new("DCA.CloudEvents.Redis", "0.0.1");
    }
}

