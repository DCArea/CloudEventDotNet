using System.Collections.Concurrent;
using System.Diagnostics;
using CloudEventDotNet.Kafka;
using Confluent.Kafka;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit.Sdk;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class KafkaPubSubTestBase
{
    public const string PubsubName = "kafka";
    public KafkaPubSubTestBase()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCloudEvents(defaultPubSubName: PubsubName, defaultTopic: "Test")
            .AddKafkaPubSub(PubsubName, opt =>
            { }, opt =>
            {
                opt.ConsumerConfig.GroupId = "Test";
                opt.RunningWorkItemLimit = 8;
            })
            .EnableDeadLetter(defaultDeadLetterTopic: "Test_DL")
            .Load(GetType().Assembly)
            .Build();

        Kafka = new FakeKafka();

        var producerFactory = A.Fake<IKafkaProducerFactory>();
        A.CallTo(producerFactory)
            .WithReturnType<IProducer<byte[], byte[]>>()
            .Returns(Kafka);
        var consumerFactory = A.Fake<IKafkaConsumerFactory>();
        A.CallTo(consumerFactory)
            .WithReturnType<IConsumer<byte[], byte[]>>()
            .ReturnsLazily(call =>
            {
                Kafka.OnPartitionAssignment = call.GetArgument<Action<IConsumer<byte[], byte[]>, List<TopicPartition>>?>(2);
                Kafka.OnPartitionRevoked = call.GetArgument<Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>>?>(4);
                return Kafka;
            });
        services.RemoveAll<IKafkaProducerFactory>();
        services.AddSingleton(producerFactory);
        services.RemoveAll<IKafkaConsumerFactory>();
        services.AddSingleton(consumerFactory);
        services.RemoveAll<ICloudEventHandlerFactory>();
        services.AddSingleton<ICloudEventHandlerFactory>(new FakeCloudEventHandlerFactory(Collector));
        ServiceProvider = services.BuildServiceProvider();

        var registry = ServiceProvider.GetRequiredService<Registry2>();
        foreach (var topic in registry.GetSubscribedTopics(PubsubName))
        {
            var tp = new TopicPartition(topic, new Partition(0));
            Kafka.Streams.Add(tp, new(tp));
        }

        Subscriber = (SubscribeHostedService)ServiceProvider.GetRequiredService<IHostedService>();
        Pubsub = ServiceProvider.GetRequiredService<ICloudEventPubSub>();
    }

    public ServiceProvider ServiceProvider { get; }
    public SubscribeHostedService Subscriber { get; }
    public ICloudEventPubSub Pubsub { get; }
    public CloudEventCollector Collector { get; } = new();
    public FakeKafka Kafka { get; }

    protected async Task StartAsync()
    {
        ((IConsumer<byte[], byte[]>)Kafka).Assign(Kafka.Streams.Keys);
        await Subscriber.StartAsync(default)
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

    protected async Task StopAsync()
    {
        await Subscriber.StopAsync(default)
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

    [StackTraceHidden]
    [DebuggerHidden]
    protected void WaitUntillDelivered<TData>(CloudEvent<TData> cloudEvent, int timeoutSeconds = 5)
        => WaitUntill(() => Collector.Delivered.Any(e => e.Id == cloudEvent.Id), timeoutSeconds);

    [StackTraceHidden]
    [DebuggerHidden]
    protected void WaitUntillProcessed<TData>(CloudEvent<TData> cloudEvent, int timeoutSeconds = 5)
        => WaitUntill(() => Collector.Processed.Any(e => e.Id == cloudEvent.Id), timeoutSeconds);

    [StackTraceHidden]
    [DebuggerHidden]
    protected static void WaitUntill(Func<bool> condition, int timeoutSeconds = 5)
    {
        if (!SpinWait.SpinUntil(condition, timeoutSeconds * 1000))
        {
            throw new XunitException($"{condition} not satisfied within {timeoutSeconds}s");
        }
    }
}

public class CloudEventCollector
{
    public ConcurrentBag<CloudEvent> Delivered { get; } = [];
    public ConcurrentBag<CloudEvent> Processed { get; } = [];
}


internal class FakeCloudEventHandlerFactory(CloudEventCollector collector) : ICloudEventHandlerFactory
{
    public ICloudEventHandler Create(IServiceProvider services, CloudEventMetadata metadata, HandleCloudEventDelegate handlerDelegate)
    {
        HandleCloudEventDelegate wrapped = async (IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token) =>
        {
            collector.Delivered.Add(@event);
            await handlerDelegate(serviceProvider, @event, token);
            collector.Processed.Add(@event);
        };
        var handler = ActivatorUtilities.CreateInstance<CloudEventHandler>(services, metadata, wrapped);
        return handler;
    }
}

