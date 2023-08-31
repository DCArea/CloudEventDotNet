using CloudEventDotNet.Kafka.Telemetry;
using Confluent.Kafka;
using DCA.Extensions.BackgroundTask;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaCloudEventSubscriber : ICloudEventSubscriber
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly Registry _registry;
    private readonly KafkaRedeliverProducer _redeliverProducer;
    private readonly string[] _topics;
    private readonly string _pubSubName;
    private readonly KafkaSubscribeOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly ILogger<KafkaCloudEventMessage> _messageLogger;
    private readonly Dictionary<TopicPartition, (KafkaMessageChannelContext Context, BackgroundTaskChannel Channel)> _channels = new();
    private readonly CancellationTokenSource _stopTokenSource = new();
    public KafkaCloudEventSubscriber(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        IKafkaConsumerFactory consumerFactory,
        ILoggerFactory loggerFactory,
        IServiceProvider serviceProvider)
    {
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KafkaCloudEventSubscriber>();
        _messageLogger = loggerFactory.CreateLogger<KafkaCloudEventMessage>();
        _registry = registry;
        _redeliverProducer = ActivatorUtilities.CreateInstance<KafkaRedeliverProducer>(serviceProvider, options);
        _topics = registry.GetSubscribedTopics(pubSubName).ToArray();

        _options.ConsumerConfig.EnableAutoOffsetStore = false;
        _consumer = consumerFactory.Create<byte[], byte[]>(
            _options.ConsumerConfig,
            errorHandler: (_, e) => Logs.ConsumerError(_logger, _pubSubName, e),
            partitionAssignmentHandler: (c, partitions) =>
            {
                Logs.PartitionsAssigned(_logger, _pubSubName, partitions);
                AssignChannels(partitions);
            },
            partitionsRevokedHandler: (c, partitions) =>
            {
                Logs.PartitionsRevoked(_logger, _pubSubName, partitions);
                RevokeChannels(partitions);
            },
            partitionsLostHandler: (c, partitions) => Logs.PartitionsLost(_logger, _pubSubName, partitions),
            logHandler: (_, log) => Logs.OnConsumerLog(_logger, _pubSubName, log)
        );
    }

    private Task _consumeLoop = default!;
    private Task _commitLoop = default!;
    public Task StartAsync()
    {
        if (_topics.Any())
        {
            _consumer.Subscribe(_topics);
            _consumeLoop = Task.Factory.StartNew(ConsumeLoop, TaskCreationOptions.LongRunning);
            _commitLoop = Task.Run(CommitLoop);
        }
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_topics.Any())
        {
            _stopTokenSource.Cancel();
            await _consumeLoop;
            await _commitLoop;
            await Task.WhenAll(_channels.Values.Select(v => v.Channel.StopAsync()));
            StoreOffsets();
            _consumer.Unsubscribe();
        }
        _consumer.Close();
    }

    private void ConsumeLoop()
    {
        _logger.LogDebug("Consume loop started");
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(_stopTokenSource.Token);
                if (consumeResult == null)
                {
                    continue;
                }
                Logs.FetchedMessage(_logger, _pubSubName, consumeResult.TopicPartitionOffset);

                var (ctx, ch) = _channels[consumeResult.TopicPartition];
                var task = new KafkaCloudEventMessage(
                    consumeResult,
                    _messageLogger,
                    ctx,
                    ch.Key,
                    _registry,
                    _redeliverProducer
                    );
                var vt = ch.DispatchAsync(task, true);
                if (!vt.IsCompletedSuccessfully)
                {
                    vt.AsTask().GetAwaiter().GetResult();
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                Logs.ConsumeFailed(_logger, e, _pubSubName);
            }
        }
        _logger.LogDebug("Consume loop stopped");
    }
    private async Task CommitLoop()
    {
        _logger.LogDebug("Started commit loop");
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                StoreOffsets();
                await Task.Delay(TimeSpan.FromSeconds(1), _stopTokenSource.Token).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in commit loop");
            }
        }
        _logger.LogDebug("Stopped commit loop");
    }

    private void AssignChannels(List<TopicPartition> topicPartitions)
    {
        foreach (var topicPartition in topicPartitions)
        {
            if (!_channels.TryGetValue(topicPartition, out var _))
            {
                _channels[topicPartition] = CreateChannel(topicPartition);
            }
        }

        (KafkaMessageChannelContext, BackgroundTaskChannel) CreateChannel(TopicPartition tp)
        {
            var context = new KafkaMessageChannelContext(
                _pubSubName,
                _consumer.Name,
                _options.ConsumerConfig.GroupId,
                tp
            );
            var channel = new BackgroundTaskChannel(
                _loggerFactory.CreateLogger<BackgroundTaskChannel>(),
                new BackgroundTaskChannelOptions()
                {
                    Key = $"{context.PubSubName}/{context.TopicPartition.Topic}/{context.TopicPartition.Partition.Value}",
                    Capacity = _options.RunningWorkItemLimit
                });
            channel.StartAsync().GetAwaiter().GetResult();
            return (context, channel);
        }
    }

    private void RevokeChannels(List<TopicPartitionOffset> tops)
    {
        var channelsToRevoke = new List<BackgroundTaskChannel>();
        foreach (var tpo in tops)
        {
            if (_channels.TryGetValue(tpo.TopicPartition, out var v))
            {
                var checkpoint = (KafkaCloudEventMessage)v.Channel.Checkpoints.First();
                _consumer.StoreOffset(checkpoint.Offset);
                _channels.Remove(tpo.TopicPartition);
                channelsToRevoke.Add(v.Channel);
            }
        }
        Task.WhenAll(channelsToRevoke.Select(ch => ch.StopAsync())).GetAwaiter().GetResult();
        StoreOffsets(channelsToRevoke);
    }

    private void StoreOffsets() => StoreOffsets(_channels.Values.Select(v => v.Channel));

    private void StoreOffsets(IEnumerable<BackgroundTaskChannel> channels)
    {
        try
        {
            var checkpoints = channels
                .Select(ch => ch.Checkpoints.FirstOrDefault())
                .Where(cp => cp != null)
                .Cast<KafkaCloudEventMessage>();
            foreach (var cp in checkpoints)
            {
                _consumer.StoreOffset(cp.Offset);
            }
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
        {
            _logger.LogWarning(ex, "Local_State Error on store offsets");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error on store offsets");
            throw;
        }
    }
}
