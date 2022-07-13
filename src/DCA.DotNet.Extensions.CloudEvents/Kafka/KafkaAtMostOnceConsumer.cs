using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal class KafkaAtMostOnceConsumer : ICloudEventSubscriber
{
    private readonly IConsumer<Ignore, byte[]> _consumer;
    private readonly ConsumerContext _consumerContext;
    private readonly SemaphoreSlim? _semaphore;
    private readonly string _pubSubName;
    private readonly KafkaSubscribeOptions _options;
    private readonly ILogger<KafkaAtMostOnceConsumer> _logger;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaAtMostOnceWorkItemManager _manager;

    public KafkaAtMostOnceConsumer(
        string pubSubName,
        KafkaSubscribeOptions options,
        ILogger<KafkaAtMostOnceConsumer> logger,
        Registry registry,
        IServiceScopeFactory scopeFactory)
    {
        _pubSubName = pubSubName;
        _options = options;
        _logger = logger;
        _registry = registry;
        _scopeFactory = scopeFactory;
        _semaphore = _options.RunningWorkItemLimit > 0 ? new SemaphoreSlim(_options.RunningWorkItemLimit) : null;
        _manager = new KafkaAtMostOnceWorkItemManager(_logger, _semaphore);
        _consumer = new ConsumerBuilder<Ignore, byte[]>(options.ConsumerConfig)
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError("Error consuming message: {Error}", e);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Assigned partitions: {Partitions}", partitions);
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
            .SetOffsetsCommittedHandler((_, offsets) =>
            {
                _logger.LogInformation("Committed offsets: {Offsets}", offsets.Offsets);
            })
        .Build();

        _consumerContext = new ConsumerContext(pubSubName, _consumer.Name, _options.ConsumerConfig.GroupId);
    }

    public async Task Subscribe(CancellationToken token)
    {

        var topics = _registry.GetTopics(_pubSubName);
        _consumer.Subscribe(topics);
        _logger.LogInformation("Consumer {name} subscribed to topics: {Topics}", _consumer.Name, topics);

        while (!token.IsCancellationRequested)
        {
            try
            {
                if (_semaphore != null)
                {
                    await _semaphore.WaitAsync(token);
                }
                _logger.LogDebug("Consuming message");
                var consumeResult = _consumer.Consume(2000);
                if (consumeResult == null)
                {
                    continue;
                }
                _logger.LogDebug("Consumed message: {offset}", consumeResult.TopicPartitionOffset);
                var workItem = new KafkaMessageWorkItem(
                    _consumerContext,
                    consumeResult,
                    _manager,
                    _registry,
                    _scopeFactory,
                    _logger);
                var vt = _manager.OnReceived(workItem);
                if (!vt.IsCompleted)
                {
                    await vt;
                }
                ThreadPool.UnsafeQueueUserWorkItem(workItem, preferLocal: false);
            }
            catch (Exception e)
            {
                if (e is OperationCanceledException oe && oe.CancellationToken.WaitHandle == token.WaitHandle)
                {
                    _logger.LogInformation("Consuming cancelled");
                    break;
                }
                _logger.LogError(e, "Error consuming message");
            }
        }
        _consumer.Close();
        await _manager.StopAsync();
    }
}
