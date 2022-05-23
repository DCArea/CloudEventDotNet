using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal sealed class KafkaAtLeastOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly string _pubSubName;
    private readonly KafkaSubscribeOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<KafkaAtMostOnceConsumer> _logger;
    private readonly KafkaAtLeastOnceWorkItemManager _manager;

    public KafkaAtLeastOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _registry = registry;
        _scopeFactory = scopeFactory;
        _pubSubName = pubSubName;
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<KafkaAtMostOnceConsumer>();
        _manager = new KafkaAtLeastOnceWorkItemManager(_loggerFactory);
        _options.ConsumerConfig.EnableAutoCommit = false;
        _consumer = new ConsumerBuilder<Ignore, byte[]>(_options.ConsumerConfig)
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError("Error consuming message: {Error}", e);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Assigned partitions: {Partitions}", partitions);
                _manager.Update(partitions);
            })
            .SetPartitionsLostHandler((c, partitions) =>
            {
                _logger.LogInformation("Lost partitions: {Partitions}", partitions);
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation("Revoked partitions: {Partitions}", partitions);
            })
            .SetLogHandler((_, log) =>
            {
                _logger.LogInformation("Log: {Log}", log);
            })
            .Build();
    }

    public async Task Subscribe(CancellationToken token)
    {
        var topics = _registry.GetTopics(_pubSubName);
        _consumer.Subscribe(topics);
        _logger.LogInformation("Consumer {name} subscribed to topics: {Topics}", _consumer.Name, topics);

        var commitTask = Task.Run(() => CommitOffsetsLoop(token), default);

        while (!token.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(10000);
                if (consumeResult == null)
                {
                    continue;
                }
                _logger.LogDebug("Consumed message: {offset}", consumeResult.TopicPartitionOffset);
                var workItem = new KafkaMessageWorkItem(
                    _pubSubName,
                    consumeResult,
                    _manager,
                    _registry,
                    _scopeFactory,
                    _logger);
                var vt = _manager.OnReceived(workItem);
                if (!vt.IsCompletedSuccessfully)
                {
                    await vt;
                }
                ThreadPool.UnsafeQueueUserWorkItem(workItem, preferLocal: false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error consuming message");
            }
        }

        _logger.LogInformation("Stoping consumer", _consumer.Name);
        _consumer.Unsubscribe();
        await _manager.StopAsync();
        _logger.LogInformation("Finished processing inflighting workitems");
        await commitTask;
        CommitOffsets();
        _consumer.Close();
    }

    public async Task CommitOffsetsLoop(CancellationToken token)
    {
        var commitTimer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await commitTimer.WaitForNextTickAsync(default))
        {
            if (token.IsCancellationRequested)
            {
                return;
            }
            var offsets = _manager.GetOffsets();
            _consumer.Commit(offsets);
        }
    }

    public void CommitOffsets()
    {
        var offsets = _manager.GetOffsets().Select(offset => new TopicPartitionOffset(offset.TopicPartition, offset.Offset + 1));
        _consumer.Commit(offsets);
        _logger.LogInformation("Committed offsets: {Offsets}", offsets);
    }
}
