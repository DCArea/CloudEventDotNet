using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Confluent.Kafka;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class FakeKafkaMessageStream
{
    public FakeKafkaMessageStream(TopicPartition topicPartition)
    {
        Stream = Channel.CreateUnbounded<ConsumeResult<byte[], byte[]>>(new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = false
        });
        TopicPartition = topicPartition;
    }
    public Channel<ConsumeResult<byte[], byte[]>> Stream { get; }

    private TopicPartition TopicPartition { get; }
    public TopicPartitionOffset TopicPartitionOffset => new(TopicPartition, offset);
    private long offset = -1;

    public TopicPartitionOffset Write(Message<byte[], byte[]> message)
    {
        var nOffset = Interlocked.Increment(ref offset);
        var tpo = new TopicPartitionOffset(TopicPartition, new Offset(nOffset));
        var consumeResult = new ConsumeResult<byte[], byte[]>()
        {
            TopicPartitionOffset = tpo,
            Message = message
        };
        Stream.Writer.TryWrite(consumeResult);
        return tpo;
    }

    public bool Consume([MaybeNullWhen(false)] out ConsumeResult<byte[], byte[]> result)
    {
        return Stream.Reader.TryRead(out result);
    }

    public void Stop()
    {
        Stream.Writer.Complete();
    }
}
