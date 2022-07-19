
using System.Reflection;
using DCA.DotNet.Extensions.CloudEvents.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DCA.DotNet.Extensions.CloudEvents;

public class PubSubBuilder
{
    private readonly string _defaultPubSubName;
    private readonly string _defaultTopic;
    private readonly string _defaultSource;

    public PubSubBuilder(IServiceCollection services, string defaultPubSubName, string defaultTopic, string defaultSource, params Assembly[] assemblies)
    {
        Services = services;
        _defaultPubSubName = defaultPubSubName;
        _defaultTopic = defaultTopic;
        _defaultSource = defaultSource;

        services.AddOptions();
        services.AddHostedService<SubscribeHostedService>();
        services.AddSingleton<ICloudEventPubSub, CloudEventPubSub>();
    }

    public IServiceCollection Services { get; }

    public PubSubBuilder Load(params Assembly[] assemblies)
    {
        if (!assemblies.Any())
        {
            throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
        }

        var registry = new Registry(_defaultPubSubName, _defaultTopic, _defaultSource);
        foreach (var type in assemblies.SelectMany(a => a.DefinedTypes))
        {
            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsAbstract || typeInfo.IsInterface || typeInfo.IsGenericTypeDefinition || typeInfo.ContainsGenericParameters)
            {
                continue;
            }

            if (type.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute)
            {
                registry.RegisterMetadata(type, attribute);
                continue;
            }

            if (!typeInfo.IsSealed)
            {
                continue;
            }

            var handlerInterfaces = type
                .GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICloudEventHandler<>))
                .ToArray();
            if (!handlerInterfaces.Any()) continue;
            foreach (var handlerInterface in handlerInterfaces)
            {
                var eventDataType = handlerInterface.GenericTypeArguments[0];
                if (eventDataType.GetCustomAttribute<CloudEventAttribute>() is CloudEventAttribute attribute2)
                {
                    registry.RegisterMetadata(eventDataType, attribute2);
                }
                else
                {
                    throw new InvalidOperationException($"Handler {type.Name} implements {handlerInterface.Name} but does not have a {nameof(CloudEventAttribute)}.");
                }
                typeof(Registry)
                    .GetMethod(nameof(Registry.RegisterHandler), BindingFlags.NonPublic | BindingFlags.Instance)!
                    .MakeGenericMethod(eventDataType)!
                    .Invoke(registry, new[] { registry.GetMetadata(eventDataType) });
                Services.AddScoped(handlerInterface, type);
            }
        }
        Services.AddSingleton(registry);
        // registry.Debug();
        return this;
    }

    public PubSubBuilder AddKafkaPubSub(
        string name,
        Action<KafkaPublishOptions>? configurePublish,
        Action<KafkaSubscribeOptions>? configureSubscribe)
    {
        if (configurePublish is not null)
        {
            Services.Configure<KafkaPublishOptions>(name, configurePublish);
            Services.Configure<PubSubOptions>(options =>
            {
                ICloudEventPublisher factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaPublishOptions>>();
                    var options = optionsFactory.Create(name);
                    return ActivatorUtilities.CreateInstance<KafkaCloudEventPublisher>(sp, options);
                }
                options.PublisherFactoris[name] = factory;
            });
        }

        if (configureSubscribe is not null)
        {
            Services.Configure<KafkaSubscribeOptions>(name, configureSubscribe);
            Services.Configure<PubSubOptions>(options =>
            {
                ICloudEventSubscriber factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaSubscribeOptions>>();
                    var options = optionsFactory.Create(name);
                    return options.DeliveryGuarantee switch
                    {
                        DeliveryGuarantee.AtMostOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtMostOnceConsumer>(sp, name, options),
                        DeliveryGuarantee.AtLeastOnce
                            => ActivatorUtilities.CreateInstance<KafkaAtLeastOnceConsumer>(sp, name, options),
                        _ => throw new NotImplementedException(),
                    };
                }
                options.SubscriberFactoris[name] = factory;
            });
        }

        return this;
    }
}
