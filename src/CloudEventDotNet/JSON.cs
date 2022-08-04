using System.Text.Json;
using System.Text.Json.Serialization;

namespace CloudEventDotNet;

internal static class JSON
{
    public static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        //Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static string Serialize<T>(T value, JsonSerializerOptions? options = null)
        => JsonSerializer.Serialize(value, options ?? DefaultJsonSerializerOptions);
    public static byte[] SerializeToUtf8Bytes<T>(T value, JsonSerializerOptions? options = null)
        => JsonSerializer.SerializeToUtf8Bytes(value, options ?? DefaultJsonSerializerOptions);

    public static T? Deserialize<T>(string value, JsonSerializerOptions? options = null)
        => JsonSerializer.Deserialize<T>(value, options ?? DefaultJsonSerializerOptions);
    public static T? Deserialize<T>(ReadOnlySpan<byte> value, JsonSerializerOptions? options = null)
        => JsonSerializer.Deserialize<T>(value, options ?? DefaultJsonSerializerOptions);
}
