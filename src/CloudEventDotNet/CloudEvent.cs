using System.Text.Json;
using System.Text.Json.Serialization;

namespace CloudEventDotNet;

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
    public Dictionary<string, JsonElement> Extensions { get; set; } = new();

    [JsonIgnore]
    public int Retry
    {
        get
        {
            return Extensions["retry"].GetInt32();
        }
        set
        {
            Extensions["retry"] = JsonSerializer.SerializeToElement(value);
        }
    }
}

/// <summary>
/// Represents a CloudEvent.
/// </summary>
/// <typeparam name="TData">CloudEvent data type</typeparam>
/// <param name="Id">CloudEvent <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#id">'id'</see> attribute,
/// This is the ID of the event. When combined with <see cref="Source"/>, this enables deduplication.</param>
/// <param name="Source">CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#source">'source'</see> attribute.
/// This describes the event producer. Often this will include information such as the type of the event source, the
/// organization publishing the event, the process that produced the event, and some unique identifiers.
/// When combined with <see cref="Id"/>, this enables deduplication.
/// </param>
/// <param name="Type">
/// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#type">'type'</see> attribute.
/// Type of occurrence which has happened.
/// Often this attribute is used for routing, observability, policy enforcement, etc.
/// </param>
/// <param name="Time">
/// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#time">'time'</see> attribute.
/// Timestamp of when the occurrence happened.
/// </param>
/// <param name="Data">
/// CloudEvent 'data' content.  The event payload. The payload depends on the type
/// and the 'schemaurl'. It is encoded into a media format which is specified by the
/// 'contenttype' attribute (e.g. application/json).
/// <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#event-data"/>
/// </param>
/// <param name="DataSchema">
/// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#dataschema">'dataschema'</see> attribute.
/// A link to the schema that the data attribute adheres to.
/// Incompatible changes to the schema SHOULD be reflected by a different URI.
/// </param>
/// <param name="Subject">
/// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#subject">'subject'</see> attribute.
/// This describes the subject of the event in the context of the event producer (identified by <see cref="Source"/>).
/// In publish-subscribe scenarios, a subscriber will typically subscribe to events emitted by a source,
/// but the source identifier alone might not be sufficient as a qualifier for any specific event if the source context has
/// internal sub-structure.
/// </param>
public sealed record CloudEvent<TData>(
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
    /// <summary>
    /// CloudEvent <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#datacontenttype">'datacontenttype'</see> attribute.
    /// This is the content type of the <see cref="Data"/> property.
    /// This attribute enables the data attribute to carry any type of content, where the
    /// format and encoding might differ from that of the chosen event format.
    /// </summary>
    [JsonPropertyName("datacontenttype")]
    public string DataContentType { get; } = "application/json";

    /// <summary>
    /// The CloudEvents specification version for this event.
    /// </summary>
    [JsonPropertyName("specversion")]
    public string SpecVersion { get; } = "1.0";

    /// <summary>
    /// Values for extension attributes.
    /// </summary>
    [JsonExtensionData]
    public Dictionary<string, object?> Extensions { get; set; } = new();
}
