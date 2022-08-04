using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

internal delegate Task HandleCloudEventDelegate(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token);

public sealed class Registry
{
    private readonly Dictionary<Type, CloudEventMetadata> _metadata = new();
    private readonly Dictionary<CloudEventMetadata, HandleCloudEventDelegate> _handlerDelegates = new();
    private readonly Dictionary<CloudEventMetadata, CloudEventHandler> _handlers = new();
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    public Registry(string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;
    }

    internal Registry Build(IServiceProvider services)
    {
        foreach (var (metadata, handlerDelegate) in _handlerDelegates)
        {
            var handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, handlerDelegate);
            _handlers.TryAdd(metadata, handler);
        }
        return this;
    }

    internal void RegisterMetadata(Type eventDataType, CloudEventAttribute attribute)
    {
        var metadata = new CloudEventMetadata(
            PubSubName: attribute.PubSubName ?? _defaultPubSubName,
            Topic: attribute.Topic ?? _defaultTopic,
            Type: attribute.Type ?? eventDataType.Name,
            Source: attribute.Source ?? _defaultSource
        );
        _metadata.TryAdd(eventDataType, metadata);
    }

    internal CloudEventMetadata GetMetadata(Type eventDataType)
    {
        return _metadata[eventDataType];
    }

    internal bool TryGetHandler(CloudEventMetadata metadata, [NotNullWhen(true)] out CloudEventHandler? handler)
    {
        return _handlers.TryGetValue(metadata, out handler);
    }

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
                Data: @event.Data.Deserialize<TData>()!,
                DataSchema: @event.DataSchema,
                Subject: @event.Subject
            );

            return serviceProvider.GetRequiredService<ICloudEventHandler<TData>>().HandleAsync(typedEvent, token);
        }
    }

    public IEnumerable<string> GetSubscribedTopics(string pubSubName)
    {
        return _handlers.Keys
            .Where(m => m.PubSubName == pubSubName)
            .Select(m => m.Topic)
            .Distinct();
    }

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
