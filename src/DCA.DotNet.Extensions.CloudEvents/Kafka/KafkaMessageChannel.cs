using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal class KafkaMessageChannel
{
    private readonly Channel<KafkaMessageWorkItem> _channel;
    private readonly CancellationTokenSource _stopTokenSource = new();
    private readonly KafkaMessageChannelContext _channelContext;
    private readonly KafkaWorkItemContext _workItemContext;
    private readonly KafkaMessageChannelTelemetry _telemetry;

    public KafkaMessageChannel(
        KafkaSubscribeOptions options,
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext workItemContext,
        KafkaMessageChannelTelemetry telemetry)
    {
        if (options.RunningWorkItemLimit > 0)
        {
            _channel = Channel.CreateBounded<KafkaMessageWorkItem>(new BoundedChannelOptions(options.RunningWorkItemLimit)
            {
                SingleReader = true,
                SingleWriter = true
            });
            telemetry.Logger.LogDebug("Created bounded channel");
        }
        else
        {
            _channel = Channel.CreateUnbounded<KafkaMessageWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
            telemetry.Logger.LogDebug("Created unbounded channel");
        }

        Reader = new KafkaMessageChannelReader(
            _channel.Reader,
            telemetry,
            _stopTokenSource.Token);
        _channelContext = channelContext;
        _workItemContext = workItemContext;
        _telemetry = telemetry;
    }

    public bool IsActive { get; }
    public Task StopAsync() => Reader.StopAsync();

    public ChannelWriter<KafkaMessageWorkItem> Writer => _channel.Writer;

    public ValueTask WriteAsync(ConsumeResult<byte[], byte[]> message)
    {
        var workItem = new KafkaMessageWorkItem(
            _channelContext,
            _workItemContext,
            _telemetry,
            message);

        if (_channel.Writer.TryWrite(workItem))
        {
            return ValueTask.CompletedTask;
        }
        else
        {
            return _channel.Writer.WriteAsync(workItem);
        }
    }

    public KafkaMessageChannelReader Reader { get; }
}
