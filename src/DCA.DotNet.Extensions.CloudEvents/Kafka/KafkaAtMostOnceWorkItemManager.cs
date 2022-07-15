using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal interface IWorkItemLifetime
{
    ValueTask OnReceived(KafkaMessageWorkItem workItem);
    ValueTask OnFinished(KafkaMessageWorkItem workItem);
    Task StopAsync();
}

internal class KafkaAtMostOnceWorkItemManager : IWorkItemLifetime
{
    private readonly ConcurrentDictionary<TopicPartitionOffset, KafkaMessageWorkItem> _runningWorkItems = new();
    private readonly ILogger _logger;
    private readonly SemaphoreSlim? _semaphore;

    public KafkaAtMostOnceWorkItemManager(ILogger logger, SemaphoreSlim? semaphore)
    {
        _logger = logger;
        _semaphore = semaphore;
    }

    public ValueTask OnReceived(KafkaMessageWorkItem workItem)
    {
        if (!_runningWorkItems.TryAdd(workItem.TopicPartitionOffset, workItem))
        {
            throw new ArgumentException("Unable to add workItem.", nameof(workItem));
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask OnFinished(KafkaMessageWorkItem workItem)
    {
        if (_runningWorkItems.TryRemove(workItem.TopicPartitionOffset, out var removed))
        {
            if (removed != workItem)
            {
                throw new ArgumentException("Unable to remove workItem.", nameof(workItem));
            }
        }
        _semaphore?.Release();
        return ValueTask.CompletedTask;
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping all running work items.");
        while (true)
        {
            var runningItems = _runningWorkItems.Values.ToList();
            if (runningItems.Count == 0)
            {
                break;
            }
            foreach (var workItem in runningItems)
            {
                if (workItem is null)
                {
                    return;
                }
                if (!workItem.Started)
                {
                    workItem.Execute();
                }
                var vt = workItem.WaitToCompleteAsync();
                if (!vt.IsCompletedSuccessfully)
                {
                    await vt;
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

}
