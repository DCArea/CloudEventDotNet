using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal partial class RedisMessageChannel
{
    private readonly Channel<RedisMessageWorkItem> _channel;
    private readonly ILogger _logger;
    private readonly CounterAggregator _counter;
    private CancellationTokenSource? _cancellationTokenSource;

    public RedisMessageChannel(
        ILoggerFactory loggerFactory,
        RedisMessageChannelContext context,
        int capacity)
    {
        _logger = loggerFactory.CreateLogger($"{nameof(RedisMessageChannel)}[{context.Topic}]");

        if (capacity > 0)
        {
            _channel = Channel.CreateBounded<RedisMessageWorkItem>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created bounded channel");
        }
        else
        {
            _channel = Channel.CreateUnbounded<RedisMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _logger.LogDebug("Created unbounded channel");
        }

        _counter = CloudEventInstruments.MessageChannelRead.FindOrCreate(new("pubsub", context.PubSubName, "topic", context.Topic));

        Activate();
    }
    public ChannelWriter<RedisMessageWorkItem> Writer => _channel.Writer;
    public async ValueTask WriteAsync(RedisMessageWorkItem item)
    {
        if (Writer.TryWrite(item))
        {
            return;
        }
        else
        {
            await Writer.WriteAsync(item);
        }
        ThreadPool.UnsafeQueueUserWorkItem(item, false);
    }

    public Task PollTask { get; private set; }

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
                if (_channel.Reader.TryRead(out var workItem))
                {
                    _counter.Add(1);
                    if (!workItem.Started)
                    {
                        Log.StartingWorkItem(_logger);
                        workItem.Execute();
                        Log.StartedWorkItem(_logger);
                    }
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        Log.WaitingWorkItemComplete(_logger);
                        await vt;
                        Log.WorkItemCompleted(_logger);
                    }
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
        catch (Exception ex)
        {
            Log.ExceptionOnPolling(_logger, ex);
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
            EventId = 10220,
            Level = LogLevel.Trace,
            Message = "Work item not started, starting")]
        public static partial void StartingWorkItem(ILogger logger);

        [LoggerMessage(
            EventId = 10230,
            Level = LogLevel.Trace,
            Message = "Work item started")]
        public static partial void StartedWorkItem(ILogger logger);

        [LoggerMessage(
            EventId = 10240,
            Level = LogLevel.Trace,
            Message = "Work item not completed, waiting")]
        public static partial void WaitingWorkItemComplete(ILogger logger);

        [LoggerMessage(
            EventId = 10250,
            Level = LogLevel.Trace,
            Message = "Work item completed")]
        public static partial void WorkItemCompleted(ILogger logger);

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

        [LoggerMessage(
            EventId = 10700,
            Level = LogLevel.Error,
            Message = "Exception on polling")]
        public static partial void ExceptionOnPolling(ILogger logger, Exception exception);
    }
}
