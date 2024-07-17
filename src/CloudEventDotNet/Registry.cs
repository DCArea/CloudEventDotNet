using System.Collections.Frozen;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

internal delegate Task HandleCloudEventDelegate(IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token);

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

/// <summary>
/// A registor CloudEvent metadata and handlers
/// </summary>
/// <remarks>
/// Constructor of Registry
/// </remarks>
/// <param name="services">Service collection</param>
/// <param name="options">Options of register</param>
internal sealed class Registry(IServiceCollection services, RegisterOptions options)
{
    private readonly Dictionary<Type, CloudEventMetadata> _metadata = [];
    private readonly Dictionary<CloudEventMetadata, (HandleCloudEventDelegate handler, SubscriptionOptions options)> _subscriptions = [];

    internal Registry2 Build(IServiceProvider services)
    {
        var handlers = new Dictionary<CloudEventMetadata, CloudEventSubscription>();
        var factory = services.GetRequiredService<ICloudEventHandlerFactory>();
        foreach (var (metadata, sub) in _subscriptions)
        {
            handlers.TryAdd(metadata, new(factory.Create(services, metadata, sub.handler), sub.options));
        }
        var reg = new Registry2(
            _metadata.ToFrozenDictionary(),
            handlers.ToFrozenDictionary()
            );
        return reg;
    }

    private CloudEventMetadata AddMetadata(Type eventDataType, CloudEventAttribute attribute)
    {
        var metadata = CloudEventMetadata.Create(
            eventDataType,
            attribute.PubSubName ?? options.DefaultPubSubName,
            attribute.Topic ?? options.DefaultTopic,
            attribute.Type ?? eventDataType.Name,
            attribute.Source ?? options.DefaultSource
            );
        _metadata.TryAdd(eventDataType, metadata);
        return metadata;
    }

    public void RegisterEventsAndHandlers()
    {
        foreach (var type in options.Assemblies.SelectMany(a => a.DefinedTypes))
        {
            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsAbstract || typeInfo.IsInterface || typeInfo.IsGenericTypeDefinition || typeInfo.ContainsGenericParameters)
            {
                continue;
            }

            if (type.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute)
            {
                AddMetadata(type, attribute);
                continue;
            }

            var handlerInterfaces = type
                .GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>))
                .ToArray();
            if (handlerInterfaces.Length == 0)
            {
                continue;
            }

            CloudEventMetadata subMeta;

            foreach (var handlerInterface in handlerInterfaces)
            {
                var eventDataType = handlerInterface.GenericTypeArguments[0];
                if (eventDataType.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute subAttr)
                {
                    subMeta = AddMetadata(eventDataType, subAttr);
                }
                else
                {
                    throw new InvalidOperationException($"Handler {type.Name} implements {handlerInterface.Name} but does not has a {nameof(CloudEventAttribute)}.");
                }
                var handlerDelegate = (HandleCloudEventDelegate)typeof(Registry)
                    .GetMethod(nameof(GetHandlerDelegate), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(eventDataType)!
                    .Invoke(null, null)!;
                var subOptions = SubscriptionOptions.Create(
                    eventDataType,
                    subAttr.EnableDeadLetter ?? options.EnableDeadLetter,
                    subAttr.DeadLetterPubSubName ?? options.DefaultDeadLetterPubSubName ?? options.DefaultPubSubName,
                    subAttr.DeadLetterSource ?? options.DefaultDeadLetterSource ?? options.DefaultSource,
                    subAttr.DeadLetterTopic ?? options.DefaultDeadLetterTopic ?? options.DefaultTopic
                    );
                _subscriptions.TryAdd(subMeta, (handlerDelegate, subOptions));
                services.AddScoped(handlerInterface, type);
            }
        }
    }

    private static HandleCloudEventDelegate GetHandlerDelegate<TData>()
    {
        return Handle;

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

}
