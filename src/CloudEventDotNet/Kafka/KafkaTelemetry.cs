using System.Diagnostics.Metrics;

namespace CloudEventDotNet.Kafka;

internal static class KafkaTelemetry
{
    public static Meter Meter { get; }
    static KafkaTelemetry()
    {
        Meter = new("DCA.CloudEvents.Kafka", "0.0.1");
    }
}

