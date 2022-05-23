using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal class KafkaAtLeastOnceWorkItemManager : IWorkItemLifetime
{
    private readonly Dictionary<TopicPartition, TopicPartitionChannel> _channels = new();
    private readonly ILogger<KafkaAtLeastOnceWorkItemManager> _logger;
    private readonly ILoggerFactory _loggerFactory;

    public KafkaAtLeastOnceWorkItemManager(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<KafkaAtLeastOnceWorkItemManager>();
    }

    public void Update(List<TopicPartition> newTopicPartitionList)
    {
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
                _channels[topicPartition] = new TopicPartitionChannel(_loggerFactory, topicPartition);
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
