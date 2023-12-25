using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

internal delegate Task HandleCloudEventDelegate(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token);

/// <summary>
/// A registry of CloudEvent metadata and handlers
/// </summary>
/// <remarks>
/// Constructor of Registry
/// </remarks>
/// <param name="defaultPubSubName">The default PubSub name</param>
/// <param name="defaultTopic">The default topic</param>
/// <param name="defaultSource">The default source</param>
public sealed class Registry(string defaultPubSubName, string defaultTopic, string defaultSource)
{
    internal readonly Dictionary<Type, CloudEventMetadata> _metadata = [];
    internal readonly Dictionary<CloudEventMetadata, HandleCloudEventDelegate> _handlerDelegates = [];
    internal readonly Dictionary<CloudEventMetadata, ICloudEventHandler> _handlers = [];

    public string DefaultPubSubName { get; } = defaultPubSubName;

    public string DefaultTopic { get; } = defaultTopic;

    public string DefaultSource { get; } = defaultSource;

    internal Registry Build(IServiceProvider services)
    {
        var factory = services.GetRequiredService<ICloudEventHandlerFactory>();
        foreach (var (metadata, handlerDelegate) in _handlerDelegates)
        {
            _handlers.TryAdd(metadata, factory.Create(services, metadata, handlerDelegate));
        }
        return this;
    }

    internal void RegisterMetadata(Type eventDataType, CloudEventAttribute attribute)
    {
        var metadata = new CloudEventMetadata(
            PubSubName: attribute.PubSubName ?? DefaultPubSubName,
            Topic: attribute.Topic ?? DefaultTopic,
            Type: attribute.Type ?? eventDataType.Name,
            Source: attribute.Source ?? DefaultSource
        );
        _metadata.TryAdd(eventDataType, metadata);
    }

    internal CloudEventMetadata GetMetadata(Type eventDataType) => _metadata[eventDataType];

    internal bool TryGetHandler(CloudEventMetadata metadata, [NotNullWhen(true)] out ICloudEventHandler? handler) => _handlers.TryGetValue(metadata, out handler);

    internal void RegisterHandler<TData>(CloudEventMetadata metadata)
    {
        _handlerDelegates.TryAdd(metadata, Handle);

        static Task Handle(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token)
        {
            var typedEvent = new CloudEvent<TData>(
                Id: @event.Id,
                Source: @event.Source,
                Type: @event.Type,
                Time: @event.Time,
                Data: @event.Data.Deserialize<TData>(JSON.DefaultJsonSerializerOptions)!,
                DataSchema: @event.DataSchema,
                Subject: @event.Subject
            )
            {
                Extensions = @event.Extensions
            };

            return serviceProvider.GetRequiredService<ICloudEventHandler<TData>>().HandleAsync(typedEvent, token);
        }
    }

    /// <summary>
    /// Get topics subscribed by specified pubsub
    /// </summary>
    /// <param name="pubSubName">The pubsub name</param>
    /// <returns></returns>
    public IEnumerable<string> GetSubscribedTopics(string pubSubName)
    {
        return _handlers.Keys
            .Where(m => m.PubSubName == pubSubName)
            .Select(m => m.Topic)
            .Distinct();
    }

    /// <summary>
    /// Show registered metadata and handlers
    /// </summary>
    /// <returns>Registered metadata and handlers</returns>
    public string Debug()
    {
        var sb = new StringBuilder();

        sb.AppendLine("Metadata:");
        foreach (var (key, value) in _metadata)
        {
            sb.AppendLine($"{key}: {value}");
        }

        sb.AppendLine("Handlers:");
        foreach (var (key, value) in _handlers)
        {
            sb.AppendLine($"{key}: {value}");
        }

        return sb.ToString();
    }

}
