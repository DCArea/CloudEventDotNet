using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace CloudEventDotNet;

internal class CloudEventDotNetException(string message) : Exception(message)
{ }

[StackTraceHidden]
internal static class ThrowHelper
{
    [DoesNotReturn]
    public static void ConfigMissing(Type eventDataType, string configName)
    {
        throw new CloudEventDotNetException($"Configuration '{configName}' is missing for {eventDataType.Name}. Either configure it in the attribute, or provide a default value in PubSubBuilder.");
    }

    [DoesNotReturn]
    public static T ConfigMissing<T>(Type eventDataType, string configName)
    {
        throw new CloudEventDotNetException($"Configuration '{configName}' is missing for {eventDataType.Name}. Either configure it in the attribute, or provide a default value in PubSubBuilder.");
    }
}
