using CloudEventDotNet.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CloudEventDotNet;

public static class RedisPubSubBuilderExtensions
{
    /// <summary>
    /// Add a redis pubsub
    /// </summary>
    /// <param name="builder">The builder to configure pubsub</param>
    /// <param name="name">The name of this pubsub</param>
    /// <param name="configurePublish">An action to configure publiser.</param>
    /// <param name="configureSubscribe">An action to configure subscriber.</param>
    /// <returns>The configured pubsub builder.</returns>
    public static PubSubBuilder AddRedisPubSub(
        this PubSubBuilder builder,
        string name,
        Action<RedisPublishOptions>? configurePublish,
        Action<RedisSubscribeOptions>? configureSubscribe)
    {
        var services = builder.Services;

        if (configurePublish is not null)
        {
            services.Configure<RedisPublishOptions>(name, configurePublish);
            builder.AddPublisher(name, factory);

            ICloudEventPublisher factory(IServiceProvider sp)
            {
                var optionsFactory = sp.GetRequiredService<IOptionsFactory<RedisPublishOptions>>();
                var options = optionsFactory.Create(name);
                return ActivatorUtilities.CreateInstance<RedisCloudEventPublisher>(sp, name, options);
            }

        }

        if (configureSubscribe is not null)
        {
            services.Configure<RedisSubscribeOptions>(name, configureSubscribe);
            builder.AddSubscriber(name, factory);
            ICloudEventSubscriber factory(IServiceProvider sp)
            {
                var optionsFactory = sp.GetRequiredService<IOptionsFactory<RedisSubscribeOptions>>();
                var options = optionsFactory.Create(name);
                return ActivatorUtilities.CreateInstance<RedisCloudEventSubscriber>(sp, name, options);
            }
        }
        return builder;
    }

}
