using System.Diagnostics;
using Xunit.Sdk;

namespace CloudEventDotNet.IntegrationTest;

internal static class WaitHelper
{
    [StackTraceHidden]
    [DebuggerHidden]
    internal static void WaitUntill(Func<bool> condition, int timeoutSeconds = 5)
    {
        if (!SpinWait.SpinUntil(condition, timeoutSeconds * 1000))
        {
            throw new XunitException($"{condition} not satisfied within {timeoutSeconds}s");
        }
    }
}
