using System.Text.Json;
using System.Text.Json.Serialization;

namespace DCA.DotNet.Extensions.CloudEvents;

internal record CloudEvent(
    [property: JsonPropertyName("id")]
    string Id,
    [property: JsonPropertyName("source")]
    string Source,
    [property: JsonPropertyName("type")]
    string Type,
    [property: JsonPropertyName("time")]
    DateTimeOffset Time,
    [property: JsonPropertyName("data")]
    JsonElement Data,
    [property: JsonPropertyName("dataschema")]
    Uri? DataSchema,
    [property: JsonPropertyName("subject")]
    string? Subject
)
{
    [JsonPropertyName("datacontenttype")]
    public string DataContentType { get; } = "application/json";

    [JsonPropertyName("specversion")]
    public string SpecVersion { get; } = "1.0";

    [JsonExtensionData]
    public Dictionary<string, object?>? Extensions { get; set; }
}

public record CloudEvent<TData>(
    [property: JsonPropertyName("id")]
    string Id,
    [property: JsonPropertyName("source")]
    string Source,
    [property: JsonPropertyName("type")]
    string Type,
    [property: JsonPropertyName("time")]
    DateTimeOffset Time,
    [property: JsonPropertyName("data")]
    TData Data,
    [property: JsonPropertyName("dataschema")]
    Uri? DataSchema,
    [property: JsonPropertyName("subject")]
    string? Subject
)
{
    [JsonPropertyName("datacontenttype")]
    public string DataContentType { get; } = "application/json";

    [JsonPropertyName("specversion")]
    public string SpecVersion { get; } = "1.0";

    [JsonExtensionData]
    public Dictionary<string, object?>? Extensions { get; set; }
}
