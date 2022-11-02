using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaAtMostOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaWorkItemContext _workItemContext;
    private readonly string[] _topics;
    private readonly KafkaMessageChannel _channel;
    private readonly KafkaConsumerTelemetry _telemetry;
    private readonly CancellationTokenSource _stopTokenSource = new();

    public KafkaAtMostOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        ILoggerFactory loggerFactory)
    {
        _telemetry = new KafkaConsumerTelemetry(pubSubName, loggerFactory);
        _consumer = new ConsumerBuilder<byte[], byte[]>(options.ConsumerConfig)
            .SetErrorHandler((_, e) => _telemetry.OnConsumerError(e))
            .SetPartitionsAssignedHandler((c, partitions) => _telemetry.OnPartitionsAssigned(partitions))
            .SetPartitionsLostHandler((c, partitions) => _telemetry.OnPartitionsLost(partitions))
            .SetPartitionsRevokedHandler((c, partitions) => _telemetry.OnPartitionsRevoked(partitions))
            .SetLogHandler((_, log) => _telemetry.OnConsumerLog(log))
            .SetOffsetsCommittedHandler((_, offsets) => _telemetry.OnConsumerOffsetsCommited(offsets))
        .Build();

        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };
        _workItemContext = new KafkaWorkItemContext(registry, new KafkaCloudEventRepublisher(options, _telemetry));
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();

        var channelContext = new KafkaMessageChannelContext(
            pubSubName,
            _consumer.Name,
            options.ConsumerConfig.GroupId,
            new TopicPartition("*", -1)
        );
        var telemetry = new KafkaMessageChannelTelemetry(
            loggerFactory,
            channelContext
        );
        _channel = new KafkaMessageChannel(
            options,
            channelContext,
            _workItemContext,
            telemetry
        );
    }

    private Task _consumeLoop = default!;
    public Task StartAsync()
    {
        if (_topics.Any())
        {
            _consumer.Subscribe(_topics);
            _consumeLoop = Task.Factory.StartNew(ConsumeLoop, TaskCreationOptions.LongRunning);
        }
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_topics.Any())
        {
            _consumer.Unsubscribe();
            _stopTokenSource.Cancel();
            await _consumeLoop;
            _consumer.Close();
            await _channel.StopAsync();
        }
        else
        {
            _consumer.Close();
        }
    }

    private void ConsumeLoop()
    {
        _telemetry.OnConsumeLoopStarted();
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(_stopTokenSource.Token);
                if (consumeResult == null)
                {
                    continue;
                }
                _telemetry.OnMessageFetched(consumeResult.TopicPartitionOffset);
                _channel.DispatchMessage(consumeResult);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                _telemetry.OnConsumeFailed(e);
            }
        }
        _telemetry.OnConsumeLoopStopped();
    }
}
