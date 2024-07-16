using CloudEventDotNet.Kafka;
using Confluent.Kafka;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class KafkaPubSubTestBase : PubSubTestBase
{
    public const string PubsubName = "kafka";
    public const string Topic = "test_topic";
    public const string Source = "test.source";

    public FakeKafka Kafka { get; } = new();

    protected override void ConfigurePubSub(IServiceCollection services)
    {
        services.AddCloudEvents(PubsubName, Topic, Source)
            .AddKafkaPubSub(PubsubName, opt =>
            { }, opt =>
            {
                opt.ConsumerConfig.GroupId = "Test";
                opt.RunningWorkItemLimit = 8;
            })
            .EnableDeadLetter(defaultDeadLetterTopic: "Test_DL")
            .Load(GetType().Assembly)
            .Build();

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
    }

    protected override async Task StartAsync()
    {
        //var registry = ServiceProvider.GetRequiredService<Registry2>();
        //foreach (var topic in registry.GetSubscribedTopics(PubsubName))
        //{
        //    var tp = new TopicPartition(topic, new Partition(0));
        //    Kafka.Streams.Add(tp, new(tp));
        //}
        //((IConsumer<byte[], byte[]>)Kafka).Assign(Kafka.Streams.Keys);
        await base.StartAsync();
    }
}
