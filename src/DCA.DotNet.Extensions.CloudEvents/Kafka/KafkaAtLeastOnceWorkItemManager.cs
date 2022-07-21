using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal class KafkaAtLeastOnceWorkItemManager : KafkaWorkItemLifetime
{
    private readonly Dictionary<TopicPartition, TopicPartitionChannel> _channels = new();
    private readonly ILogger<KafkaAtLeastOnceWorkItemManager> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly KafkaSubscribeOptions _options;

    public KafkaAtLeastOnceWorkItemManager(ILoggerFactory loggerFactory, KafkaSubscribeOptions options)
    {
        _loggerFactory = loggerFactory;
        _options = options;
        _logger = _loggerFactory.CreateLogger<KafkaAtLeastOnceWorkItemManager>();
    }

    public void Update(List<TopicPartition> newTopicPartitionList)
    {
        StopAsync().GetAwaiter().GetResult();
        _logger.LogInformation("Updating topic partition channels");
        foreach (var channel in _channels.Where(kvp => !newTopicPartitionList.Contains(kvp.Key)))
        {
            channel.Value.Deactivate();
        }

        foreach (var topicPartition in newTopicPartitionList)
        {
            if (_channels.TryGetValue(topicPartition, out var channel))
            {
                channel.Activate();
            }
            else
            {
                _channels[topicPartition] = new TopicPartitionChannel(_loggerFactory, topicPartition, _options);
            }
        }
    }

    public Task StopAsync()
    {
        foreach (var (_, channel) in _channels)
        {
            channel.Deactivate();
        }
        return Task.WhenAll(_channels.Values.Select(c => c.PollTask));
    }

    public ValueTask OnReceived(KafkaMessageWorkItem workItem)
    {
        var tp = workItem.TopicPartition;
        var writer = _channels[tp].Writer;
        if (writer.TryWrite(workItem))
        {
            return ValueTask.CompletedTask;
        }
        else
        {
            return writer.WriteAsync(workItem);
        }
    }

    public IEnumerable<TopicPartitionOffset> GetOffsets()
    {
        return _channels.Values.Select(c => c.Offset).Where(o => o != null).ToList()!;
    }

    public ValueTask OnFinished(KafkaMessageWorkItem workItem) => ValueTask.CompletedTask;
}
