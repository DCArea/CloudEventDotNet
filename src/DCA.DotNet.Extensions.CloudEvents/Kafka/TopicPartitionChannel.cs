using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal partial class TopicPartitionChannel
{
    private readonly Channel<KafkaMessageWorkItem> _channel;
    private readonly TopicPartition _topicPartition;
    private readonly KafkaSubscribeOptions _options;
    private readonly ILogger _logger;
    private CancellationTokenSource? _cancellationTokenSource;

    public TopicPartitionChannel(
        ILoggerFactory loggerFactory,
        TopicPartition topicPartition,
        KafkaSubscribeOptions options)
    {
        _topicPartition = topicPartition;
        _options = options;
        _logger = loggerFactory.CreateLogger($"{nameof(TopicPartitionChannel)}[{_topicPartition.Topic}:{_topicPartition.Partition.Value}]");

        if (_options.RunningWorkItemLimit > 0)
        {
            _channel = Channel.CreateBounded<KafkaMessageWorkItem>(new BoundedChannelOptions(_options.RunningWorkItemLimit)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created bounded channel");
        }
        else
        {
            _channel = Channel.CreateUnbounded<KafkaMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created unbounded channel");
        }

        Activate();
    }
    public ChannelWriter<KafkaMessageWorkItem> Writer => _channel.Writer;

    public Task PollTask { get; private set; }
    public TopicPartitionOffset? Offset { get; private set; }

    [MemberNotNull(nameof(PollTask))]
    public void Activate()
    {
        _cancellationTokenSource = new CancellationTokenSource();
        var previousTask = PollTask;
        PollTask = Task.Run(() => Poll(previousTask, _cancellationTokenSource.Token));
        Log.Activated(_logger);
    }

    public void Deactivate()
    {
        _cancellationTokenSource?.Cancel();
    }

    public async Task Poll(Task? previousTask, CancellationToken token)
    {
        try
        {
            Log.PollingStarted(_logger);
            if (previousTask != null && !previousTask.IsCompleted)
            {
                Log.AwaitingPrevious(_logger);
                await previousTask;
            }

            while (true)
            {
                if (_channel.Reader.TryRead(out var item))
                {
                    if (!item.Started)
                    {
                        item.Execute();
                    }
                    if (!item.Task.IsCompleted)
                    {
                        await item.Task;
                    }
                    Offset = item.TopicPartitionOffset;
                    Log.CheckedOffset(_logger, Offset.Offset);
                }
                else
                {
                    if (token.IsCancellationRequested)
                    {
                        Log.PoolCancelled(_logger);
                        return;
                    }
                    else
                    {
                        Log.WaitingForNext(_logger);
                        await _channel.Reader.WaitToReadAsync(token);
                    }
                }
            }
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == token)
        {
            Log.PoolCancelled(_logger);
        }
    }

    public static partial class Log
    {
        [LoggerMessage(
            EventId = 10000,
            Level = LogLevel.Debug,
            Message = "Activated")]
        public static partial void Activated(ILogger logger);

        [LoggerMessage(
            EventId = 10100,
            Level = LogLevel.Trace,
            Message = "Polling started")]
        public static partial void PollingStarted(ILogger logger);

        [LoggerMessage(
            EventId = 10200,
            Level = LogLevel.Trace,
            Message = "Awaiting previous poll task")]
        public static partial void AwaitingPrevious(ILogger logger);

        [LoggerMessage(
            EventId = 10300,
            Level = LogLevel.Trace,
            Message = "Checked offset {offset}")]
        public static partial void CheckedOffset(ILogger logger, Offset offset);

        [LoggerMessage(
            EventId = 10400,
            Level = LogLevel.Trace,
            Message = "Poll cancelled")]
        public static partial void PoolCancelled(ILogger logger);

        [LoggerMessage(
            EventId = 10500,
            Level = LogLevel.Trace,
            Message = "Waiting for next work item")]
        public static partial void WaitingForNext(ILogger logger);
    }
}
