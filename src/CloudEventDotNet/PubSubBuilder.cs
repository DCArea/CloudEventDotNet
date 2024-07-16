
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Polly;

namespace CloudEventDotNet;

/// <summary>
/// A builder for configuring pubsub
/// </summary>
public class PubSubBuilder
{
    private readonly PubSubOptionsBuilder optionsBuilder = new();

    /// <summary>
    /// PubSub builder
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="defaultPubSubName">The default PubSub name</param>
    /// <param name="defaultTopic">The default topic</param>
    /// <param name="defaultSource">The default source</param>
    public PubSubBuilder(
        IServiceCollection services,
        string defaultPubSubName,
        string defaultTopic,
        string defaultSource)
    {
        Services = services;
        optionsBuilder.DefaultPubSubName = defaultPubSubName;
        optionsBuilder.DefaultTopic = defaultTopic;
        optionsBuilder.DefaultSource = defaultSource;

        services.AddHostedService<SubscribeHostedService>();
        services.AddSingleton<ICloudEventPubSub, CloudEventPubSub>();
        services.AddSingleton<ICloudEventHandlerFactory, CloudEventHandlerFactory>();
        services.AddResiliencePipelineRegistry<string>();
    }

    /// <summary>
    /// Gets an <see cref="IServiceProvider"/> which can be used to resolve services
    /// from the dependency injection container.
    /// </summary>
    public IServiceCollection Services { get; }

    public PubSubBuilder Load(params Assembly[] assemblies)
    {
        foreach (var assembly in assemblies)
        {
            if (!optionsBuilder.Assemblies.Any(a => a == assembly))
            {
                optionsBuilder.Assemblies.Add(assembly);
            }
        }
        return this;
    }

    public void Build()
    {
        Services.AddSingleton(optionsBuilder.Build);

        if (optionsBuilder.Assemblies is null)
        {
            throw new InvalidOperationException("No assemblies to register");
        }
        var register = new Registry(Services, new RegisterOptions(optionsBuilder.Assemblies.ToArray())
        {
            DefaultPubSubName = optionsBuilder.DefaultPubSubName,
            DefaultTopic = optionsBuilder.DefaultTopic,
            DefaultSource = optionsBuilder.DefaultSource,
            EnableDeadLetter = optionsBuilder.EnableDeadLetter,
            DefaultDeadLetterPubSubName = optionsBuilder.DefaultDeadLetterPubSubName,
            DefaultDeadLetterSource = optionsBuilder.DefaultDeadLetterSource,
            DefaultDeadLetterTopic = optionsBuilder.DefaultDeadLetterTopic
        });
        register.RegisterEventsAndHandlers();

        Services.AddSingleton(register.Build);
    }

    public PubSubBuilder EnableDeadLetter(
        string? defaultDeadLetterPubSubName = null,
        string? defaultDeadLetterSource = null,
        string? defaultDeadLetterTopic = null)
    {
        var services = Services;
        services.AddSingleton<IDeadLetterSender, PubSubDeadLetterSender>();

        optionsBuilder.EnableDeadLetter = true;
        optionsBuilder.DefaultDeadLetterPubSubName = defaultDeadLetterPubSubName;
        optionsBuilder.DefaultDeadLetterSource = defaultDeadLetterSource;
        optionsBuilder.DefaultDeadLetterTopic = defaultDeadLetterTopic;

        return this;
    }

    public PubSubBuilder AddDefaultResiliencePipeline(Action<ResiliencePipelineBuilder> configure)
    {
        Services.AddResiliencePipeline(CloudEventHandlerFactory.ResiliencePolicyName, configure);
        return this;
    }

    public PubSubBuilder AddPublisher(string name, Func<IServiceProvider, ICloudEventPublisher> factory)
    {
        optionsBuilder.PublisherFactories[name] = factory;
        return this;
    }
    public PubSubBuilder AddSubscriber(string name, Func<IServiceProvider, ICloudEventSubscriber> factory)
    {
        optionsBuilder.SubscriberFactories[name] = factory;
        return this;
    }
}
