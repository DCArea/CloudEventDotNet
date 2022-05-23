
using System.Diagnostics.Metrics;

namespace DCA.DotNet.Extensions.CloudEvents;

internal static class Metrics
{
    public static Meter Meter = new Meter("DCA.CloudEvents", "0.0.1");

    public static Histogram<long> ProcessLatency = Meter.CreateHistogram<long>("dca_cloudevents_process_latency");
}
