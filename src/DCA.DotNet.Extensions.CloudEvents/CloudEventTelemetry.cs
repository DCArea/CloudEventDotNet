
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace DCA.DotNet.Extensions.CloudEvents;

internal static partial class CloudEventTelemetry
{
    internal static Meter Meter { get; }
    internal static ActivitySource Source { get; }

    static CloudEventTelemetry()
    {
        Meter = new("DCA.CloudEvents", "0.0.1");
        Source = new("DCA.CloudEvents");
    }

}
