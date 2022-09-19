using System.Threading.Channels;

namespace CloudEventDotNet.Redis;

internal sealed partial class RedisMessageChannelReader
{
    private readonly ChannelReader<RedisMessageWorkItem> _channelReader;
    private readonly CancellationToken _stopToken;
    private readonly RedisMessageTelemetry _telemetry;
    private readonly Task _readLoop;

    public RedisMessageChannelReader(
        ChannelReader<RedisMessageWorkItem> channelReader,
        RedisMessageTelemetry telemetry,
        CancellationToken stopToken)
    {
        _channelReader = channelReader;
        _stopToken = stopToken;
        _telemetry = telemetry;
        _readLoop = Task.Run(ReadLoop, default);
    }

    public Task StopAsync()
    {
        return _readLoop;
    }

    private async Task ReadLoop()
    {
        try
        {
            _telemetry.OnMessageChannelReaderStarted();
            while (true)
            {
                if (_channelReader.TryRead(out var workItem))
                {
                    if (!workItem.Started)
                    {
                        _telemetry.OnWorkItemStarting();
                        workItem.Execute();
                        _telemetry.OnWorkItemStarted();
                    }
                    var vt = workItem.WaitToCompleteAsync();
                    if (!vt.IsCompletedSuccessfully)
                    {
                        _telemetry.OnWaitingWorkItemComplete();
                        await vt.ConfigureAwait(false);
                        _telemetry.OnWorkItemCompleted();
                    }
                }
                else
                {
                    if (_stopToken.IsCancellationRequested)
                    {
                        _telemetry.MessageChannelReaderStopped();
                        return;
                    }
                    else
                    {
                        _telemetry.WaitingForNextWorkItem();
                        await _channelReader.WaitToReadAsync(_stopToken).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _stopToken)
        {
            _telemetry.MessageChannelReaderCancelled();
        }
        catch (Exception ex)
        {
            _telemetry.ExceptionOnReadingWorkItems(ex);
        }
        _telemetry.MessageChannelReaderStopped();
    }
}
