
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace CloudEventDotNet;

/// <summary>
/// A builder for configuring pubsub
/// </summary>
public class PubSubBuilder
{
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    /// <summary>
    /// PubSub builder
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="defaultPubSubName">The default PubSub name</param>
    /// <param name="defaultTopic">The default topic</param>
    /// <param name="defaultSource">The default source</param>
    public PubSubBuilder(IServiceCollection services, string defaultPubSubName, string defaultTopic, string defaultSource)
    {
        Services = services;
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;

        services.AddOptions();
        services.Configure<PubSubOptions>(opts =>
        {
            opts.DefaultPubSubName = _defaultPubSubName;
            opts.DefaultTopic = _defaultTopic;
            opts.DefaultSource = _defaultSource;
        });
        services.AddHostedService<SubscribeHostedService>();
        services.AddSingleton<ICloudEventPubSub, CloudEventPubSub>();
        services.AddSingleton<ICloudEventHandlerFactory, CloudEventHandlerFactory>();
    }

    /// <summary>
    /// Gets an <see cref="IServiceProvider"/> which can be used to resolve services
    /// from the dependency injection container.
    /// </summary>
    public IServiceCollection Services { get; }

    internal Registry? Registry { get; set; }

    /// <summary>
    /// Load cloudevents metadata from specifed assemblies.
    /// </summary>
    /// <param name="assemblies">Assemblies to scan</param>
    /// <returns>PubSub builder</returns>
    /// <exception cref="ArgumentException">No assemblies found to scan</exception>
    /// <exception cref="InvalidOperationException">CloudEvent handler registered with unknown CloudEvent</exception>
    public PubSubBuilder Load(params Assembly[] assemblies)
    {
        if (!assemblies.Any())
        {
            throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
        }

        Registry = new Registry(_defaultPubSubName, _defaultTopic, _defaultSource);
        foreach (var type in assemblies.SelectMany(a => a.DefinedTypes))
        {
            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsAbstract || typeInfo.IsInterface || typeInfo.IsGenericTypeDefinition || typeInfo.ContainsGenericParameters)
            {
                continue;
            }

            if (type.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute)
            {
                Registry.RegisterMetadata(type, attribute);
                continue;
            }

            var handlerInterfaces = type
                .GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>))
                .ToArray();
            if (!handlerInterfaces.Any())
            {
                continue;
            }

            foreach (var handlerInterface in handlerInterfaces)
            {
                var eventDataType = handlerInterface.GenericTypeArguments[0];
                if (eventDataType.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute2)
                {
                    Registry.RegisterMetadata(eventDataType, attribute2);
                }
                else
                {
                    throw new InvalidOperationException($"Handler {type.Name} implements {handlerInterface.Name} but does not have a {nameof(CloudEventAttribute)}.");
                }
                typeof(Registry)
                    .GetMethod(nameof(Registry.RegisterHandler), BindingFlags.NonPublic | BindingFlags.Instance)!
                    .MakeGenericMethod(eventDataType)!
                    .Invoke(Registry, new[] { (object)Registry.GetMetadata(eventDataType) });
                Services.AddScoped(handlerInterface, type);
            }
        }
        Services.AddSingleton(Registry.Build);
        return this;
    }

    public PubSubBuilder AddPubSubDeadLetterSender(
        Action<PubSubDeadLetterSenderOptions> configure)
    {
        var services = Services;

        services.AddSingleton<IDeadLetterSender, PubSubDeadLetterSender>();
        services.Configure(configure);

        return this;
    }
}
