using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal sealed class KafkaAtLeastOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaWorkItemContext _workItemContext;
    private readonly string[] _topics;
    private readonly KafkaConsumerTelemetry _telemetry;
    private readonly string _pubSubName;
    private readonly KafkaSubscribeOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<TopicPartition, KafkaMessageChannel> _channels = new();
    private readonly CancellationTokenSource _stopTokenSource = new();
    public KafkaAtLeastOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _telemetry = new KafkaConsumerTelemetry(pubSubName, loggerFactory);

        _options.ConsumerConfig.EnableAutoCommit = false;
        _consumer = new ConsumerBuilder<byte[], byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) => _telemetry.OnConsumerError(e))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _telemetry.OnPartitionsAssigned(partitions);
                UpdateChannels(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) => _telemetry.OnPartitionsLost(partitions))
            .SetPartitionsRevokedHandler((c, partitions) => _telemetry.OnPartitionsRevoked(partitions))
            .SetLogHandler((_, log) => _telemetry.OnConsumerLog(log))
            .Build();

        _workItemContext = new KafkaWorkItemContext(registry, new(options, _telemetry));
        _topics = registry.GetTopics(pubSubName).ToArray();
    }

    private Task _consumeLoop = default!;
    private Task _commitLoop = default!;
    public Task StartAsync()
    {
        _consumer.Subscribe(_topics);
        _consumeLoop = Task.Factory.StartNew(ConsumeLoop, TaskCreationOptions.LongRunning);
        _commitLoop = Task.Run(CommitLoop);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _consumer.Unsubscribe();
        _stopTokenSource.Cancel();
        await _consumeLoop;
        await _commitLoop;
        await Task.WhenAll(_channels.Values.Select(ch => ch.StopAsync()));
        CommitOffsets();
        _consumer.Close();
    }

    private void ConsumeLoop()
    {
        _telemetry.OnConsumeLoopStarted();
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                ConsumeResult<byte[], byte[]> consumeResult = _consumer.Consume(10000);
                if (consumeResult == null)
                {
                    continue;
                }
                _telemetry.OnMessageFetched(consumeResult.TopicPartitionOffset);

                var channel = _channels[consumeResult.TopicPartition];
                var vt = channel.WriteAsync(consumeResult);
                if (!vt.IsCompletedSuccessfully)
                {
                    vt.ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                _telemetry.OnConsumeFailed(e);
            }
        }
        _telemetry.OnConsumeLoopStopped();
    }

    private void UpdateChannels(List<TopicPartition> topicPartitions)
    {
        foreach (var topicPartition in topicPartitions)
        {
            if (!_channels.TryGetValue(topicPartition, out var _))
            {
                _channels[topicPartition] = StartChannel(topicPartition);
            }
        }

        var channelsToStopped = new List<KafkaMessageChannel>();
        foreach (var (tp, channel) in _channels.Where(kvp => !topicPartitions.Contains(kvp.Key)))
        {
            _channels.Remove(tp);
            channelsToStopped.Add(channel);
        }

        // wait all pending messages checked
        Task.WhenAll(channelsToStopped.Select(ch => ch.StopAsync())).GetAwaiter().GetResult();
        CommitOffsets(channelsToStopped);

        KafkaMessageChannel StartChannel(TopicPartition tp)
        {
            var channelContext = new KafkaMessageChannelContext(
                _pubSubName,
                _consumer.Name,
                _options.ConsumerConfig.GroupId,
                tp
            );
            var telemetry = new KafkaMessageChannelTelemetry(
                _loggerFactory,
                channelContext
            );
            return new KafkaMessageChannel(
                _options,
                channelContext,
                _workItemContext,
                telemetry
            );
        }
    }

    private async Task CommitLoop()
    {
        _telemetry.OnCommitLoopStarted();
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                CommitOffsets();
                await Task.Delay(TimeSpan.FromSeconds(10), _stopTokenSource.Token).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { break; }
            catch (Exception ex)
            {
                _telemetry.OnCommitLoopError(ex);
            }
        }
        _telemetry.OnCommitLoopStopped();
    }

    private void CommitOffsets() => CommitOffsets(_channels.Values);

    private void CommitOffsets(IEnumerable<KafkaMessageChannel> channels)
    {
        try
        {
            var offsets = channels
                .Where(ch => ch.Reader.Offset != null)
                .Select(ch => ch.Reader.Offset)
                .Select(offset => new TopicPartitionOffset(offset!.TopicPartition, offset.Offset + 1))
                .ToArray();
            _consumer.Commit(offsets);
            _telemetry.OnOffsetsCommited(offsets);
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error on commit offsets");
            throw;
        }
    }
}
