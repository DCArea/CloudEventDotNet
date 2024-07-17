using System.Reflection;

namespace CloudEventDotNet;

internal class RegisterOptions(Assembly[] assemblies)
{
    public Assembly[] Assemblies => assemblies;

    public string? DefaultPubSubName { get; init; }
    public string? DefaultTopic { get; init; }
    public string? DefaultSource { get; init; }

    public bool EnableDeadLetter { get; init; } = true;
    public string? DefaultDeadLetterPubSubName { get; init; }
    public string? DefaultDeadLetterSource { get; init; }
    public string? DefaultDeadLetterTopic { get; init; }
}
