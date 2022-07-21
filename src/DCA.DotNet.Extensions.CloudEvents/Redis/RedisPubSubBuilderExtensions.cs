using DCA.DotNet.Extensions.CloudEvents.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DCA.DotNet.Extensions.CloudEvents;
public static class RedisPubSubBuilderExtensions
{
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
            services.Configure<PubSubOptions>(options =>
            {
                ICloudEventPublisher factory(IServiceProvider sp)
                {
                    var optionsFactory = sp.GetRequiredService<IOptionsFactory<RedisPublishOptions>>();
                    var options = optionsFactory.Create(name);
                    return ActivatorUtilities.CreateInstance<RedisCloudEventPublisher>(sp, options);
                }
                options.PublisherFactoris[name] = factory;
            });
        }

        if (configureSubscribe is not null)
        {
            services.Configure<RedisSubscribeOptions>(name, configureSubscribe);
            services.Configure<PubSubOptions>(options =>
            {
                ICloudEventSubscriber factory(IServiceProvider sp)
                {
                    return ActivatorUtilities.CreateInstance<RedisCloudEventSubscriber>(sp, name);
                }
                options.SubscriberFactoris[name] = factory;
            });
            services.AddSingleton<RedisProcessChannelFactory>();
        }

        return builder;
    }
}
