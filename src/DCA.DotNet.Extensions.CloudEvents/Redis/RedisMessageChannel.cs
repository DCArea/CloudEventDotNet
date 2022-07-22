using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal partial class RedisMessageChannel
{
    private readonly Channel<RedisMessageWorkItem> _channel;
    private readonly RedisMessageTelemetry _telemetry;
    private readonly CancellationTokenSource _stopTokenSource = new();

    public RedisMessageChannel(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext workItemContext,
        RedisMessageTelemetry telemetry)
    {
        _telemetry = telemetry;

        int capacity = options.RunningWorkItemLimit;
        if (capacity > 0)
        {
            _channel = Channel.CreateBounded<RedisMessageWorkItem>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true
            });
            _telemetry.Logger.LogDebug("Created bounded channel");
        }
        else
        {
            _channel = Channel.CreateUnbounded<RedisMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            _telemetry.Logger.LogDebug("Created unbounded channel");
        }

        Writer = new RedisMessageChannelWriter(
            options,
            database,
            channelContext,
            _channel.Writer,
            workItemContext,
            _telemetry,
            _stopTokenSource.Token);

        Reader = new RedisMessageChannelReader(
            _channel.Reader,
            _telemetry,
            _stopTokenSource.Token);
    }

    public RedisMessageChannelWriter Writer { get; }
    public RedisMessageChannelReader Reader { get; }

    public async Task StopAsync()
    {
        await Writer.StopAsync();
        await Reader.StopAsync();
    }
}
