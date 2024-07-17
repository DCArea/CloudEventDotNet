using System.Diagnostics;
using Xunit.Sdk;

namespace CloudEventDotNet.IntegrationTest;

internal static class WaitHelper
{
    [StackTraceHidden]
    [DebuggerHidden]
    internal static void WaitUntill(Func<bool> condition, int timeoutSeconds = 5)
    {
        if (timeoutSeconds > 30)
        {
            throw new ArgumentOutOfRangeException(nameof(timeoutSeconds), "timeout should not large than 30s");
        }
        var timeout = Debugger.IsAttached ? Timeout.Infinite : timeoutSeconds * 1000;
        if (!SpinWait.SpinUntil(condition, timeout))
        {
            throw new XunitException($"{condition} not satisfied within {timeoutSeconds}s");
        }
    }
}
