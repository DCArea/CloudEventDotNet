namespace DCA.DotNet.Extensions.CloudEvents;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CloudEventAttribute : Attribute
{
    public string? Type { get; init; }
    public string? Source { get; init; }
    public string? PubSubName { get; init; }
    public string? Topic { get; init; }
}

public record CloudEventMetadata(
    string PubSubName,
    string Topic,
    string Type,
    string Source
);
