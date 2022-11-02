
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

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
