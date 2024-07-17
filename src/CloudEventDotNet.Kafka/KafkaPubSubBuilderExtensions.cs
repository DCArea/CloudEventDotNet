using CloudEventDotNet.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;
public static class KafkaPubSubBuilderExtensions
{
    /// <summary>
    /// Add a kafka pubsub
    /// </summary>
    /// <param name="builder">The builder to configure pubsub</param>
    /// <param name="name">The name of this pubsub</param>
    /// <param name="configurePublish">An action to configure publiser.</param>
    /// <param name="configureSubscribe">An action to configure subscriber.</param>
    /// <returns>The configured pubsub builder.</returns>
    public static PubSubBuilder AddKafkaPubSub(
        this PubSubBuilder builder,
        string name,
        Action<KafkaPublishOptions>? configurePublish,
        Action<KafkaSubscribeOptions>? configureSubscribe)
    {
        var services = builder.Services;

        if (configurePublish is not null)
        {
            services.AddSingleton<IKafkaProducerFactory, KafkaProducerFactory>();
            services.Configure<KafkaPublishOptions>(name, configurePublish);
            builder.AddPublisher(name, factory);
            ICloudEventPublisher factory(IServiceProvider sp)
            {
                var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaPublishOptions>>();
                var options = optionsFactory.Create(name);
                return ActivatorUtilities.CreateInstance<KafkaCloudEventPublisher>(sp, name, options);
            }
        }

        if (configureSubscribe is not null)
        {
            services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
            services.Configure<KafkaSubscribeOptions>(name, configureSubscribe);
            builder.AddSubscriber(name, factory);
            ICloudEventSubscriber factory(IServiceProvider sp)
            {
                var optionsFactory = sp.GetRequiredService<IOptionsFactory<KafkaSubscribeOptions>>();
                var options = optionsFactory.Create(name);

                return ActivatorUtilities.CreateInstance<KafkaCloudEventSubscriber>(sp, name, options);
            }
        }

        return builder;
    }
}
