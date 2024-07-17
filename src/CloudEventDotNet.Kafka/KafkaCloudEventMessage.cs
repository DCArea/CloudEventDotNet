using System.Diagnostics;
using CloudEventDotNet.Kafka.Telemetry;
using Confluent.Kafka;
using DCA.Extensions.BackgroundTask;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaCloudEventMessage(
    ConsumeResult<byte[], byte[]> message,
    ILogger<KafkaCloudEventMessage> logger,
    KafkaMessageChannelContext channelContext,
    string channelKey,
    Registry registry,
    KafkaRedeliverProducer producer
    ) : IBackgroundTask
{
    private readonly WorkItemWaiter _waiter = new();
    private int _started = 0;
    public bool Started => _started == 1;
    public ConsumeResult<byte[], byte[]> Message => message;
    void IThreadPoolWorkItem.Execute() => Start();
    public void Start()
    {
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }
    public ValueTask WaitToCompleteAsync() => _waiter.Task;

    private readonly CancellationTokenSource _cancellationTokenSource = new();
    public async Task ExecuteAsync()
    {
        Activity? activity = null;
        try
        {
            var cloudEvent = JSON.Deserialize<CloudEvent>(message.Message.Value)!;
            var metadata = new CloudEventMetadata(channelContext.PubSubName, message.Topic, cloudEvent.Type, cloudEvent.Source);

            if (!registry.TryGetSubscription(metadata, out var sub))
            {
                CloudEventDotNet.Telemetry.Logs.CloudEventHandlerNotFound(logger, channelKey, metadata);
                return;
            }

            activity = CloudEventDotNet.Telemetry.Tracing.OnProcessing(channelContext.PubSubName, channelContext.TopicPartition.Topic, cloudEvent);
            if (activity is not null)
            {
                Tracing.OnMessageProcessing(activity, channelContext.ConsumerGroup, message.TopicPartitionOffset);
            }

            var result = await sub.Handler.ProcessAsync(cloudEvent, sub.Options, _cancellationTokenSource.Token)
                .ConfigureAwait(false);

            if (result == ProcessingResult.Failed)
            {
                await producer.ReproduceAsync(message)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, $"Exception: {ex.GetType().Name}");
            logger.LogError(ex, "Error on process kafka message {offset}", message.TopicPartitionOffset);
        }
        finally
        {
            activity?.Dispose();
            _waiter.SetResult();
        }
    }

}
