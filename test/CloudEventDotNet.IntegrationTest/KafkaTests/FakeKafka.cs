using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class FakeKafka : IConsumer<byte[], byte[]>, IProducer<byte[], byte[]>
{
    //public Dictionary<TopicPartition, FakeKafkaMessageStream> Streams { get; set; } = [];
    public ConcurrentDictionary<string, FakeKafkaMessageStream[]> Streams { get; set; } = [];
    public ConcurrentBag<DeliveryResult<byte[], byte[]>> ProducedMessages { get; } = [];
    public ConcurrentBag<Message<byte[], byte[]>> ConsumedMessages { get; } = [];
    public Action<IConsumer<byte[], byte[]>, List<TopicPartition>>? OnPartitionAssignment { get; set; }
    public Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>>? OnPartitionRevoked { get; set; }
    string IConsumer<byte[], byte[]>.MemberId => throw new NotImplementedException();

    public List<TopicPartition> Assignment { get; private set; } = [];

    List<string> IConsumer<byte[], byte[]>.Subscription { get; } = [];

    IConsumerGroupMetadata IConsumer<byte[], byte[]>.ConsumerGroupMetadata => throw new NotImplementedException();

    Handle IClient.Handle => throw new NotImplementedException();

    string IClient.Name { get; } = "Test";

    void IConsumer<byte[], byte[]>.Assign(TopicPartition partition) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Assign(TopicPartitionOffset partition) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Assign(IEnumerable<TopicPartition> partitions)
    //=> throw new NotImplementedException();
    {
        var partitionList = partitions.ToList();
        var revokedPartitions = Assignment.Except(partitionList)
            .Select(tp => Streams[tp.Topic][tp.Partition.Value].TopicPartitionOffset)
            .ToList();
        OnPartitionRevoked?.Invoke(this, revokedPartitions);
        OnPartitionAssignment?.Invoke(this, partitionList);
        this.Assignment = partitionList;
    }

    void IConsumer<byte[], byte[]>.Close() { }
    List<TopicPartitionOffset> IConsumer<byte[], byte[]>.Commit() => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Commit(IEnumerable<TopicPartitionOffset> offsets) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Commit(ConsumeResult<byte[], byte[]> result) => throw new NotImplementedException();
    List<TopicPartitionOffset> IConsumer<byte[], byte[]>.Committed(TimeSpan timeout) => throw new NotImplementedException();
    List<TopicPartitionOffset> IConsumer<byte[], byte[]>.Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotImplementedException();

    private bool TryConsume([MaybeNullWhen(false)] out ConsumeResult<byte[], byte[]> item)
    {
        foreach (var stream in Streams
            .Where(kvp => ((IConsumer<byte[], byte[]>)this).Subscription.Contains(kvp.Key))
            .SelectMany(kvp => kvp.Value))
        {
            if (stream.Consume(out item))
            {
                return true;
            }
        }
        item = null;
        return false;
    }

    ConsumeResult<byte[], byte[]> IConsumer<byte[], byte[]>.Consume(int millisecondsTimeout)
    {
        var cancel = new CancellationTokenSource(millisecondsTimeout);
        return ((IConsumer<byte[], byte[]>)this).Consume(cancel.Token);
    }

    ConsumeResult<byte[], byte[]>? IConsumer<byte[], byte[]>.Consume(CancellationToken cancellationToken)
    {
        var wait = new SpinWait();
        while (!cancellationToken.IsCancellationRequested)
        {
            if (TryConsume(out var item))
            {
                return item;
            }
            wait.SpinOnce(100);
        }
        return null;
    }

    ConsumeResult<byte[], byte[]> IConsumer<byte[], byte[]>.Consume(TimeSpan timeout)
    {
        var cancel = new CancellationTokenSource(timeout);
        return ((IConsumer<byte[], byte[]>)this).Consume(cancel.Token);
    }

    public void Dispose() => throw new NotImplementedException();
    WatermarkOffsets IConsumer<byte[], byte[]>.GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotImplementedException();
    List<TopicPartitionOffset> IConsumer<byte[], byte[]>.OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Pause(IEnumerable<TopicPartition> partitions) => throw new NotImplementedException();
    Offset IConsumer<byte[], byte[]>.Position(TopicPartition partition) => throw new NotImplementedException();
    WatermarkOffsets IConsumer<byte[], byte[]>.QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Resume(IEnumerable<TopicPartition> partitions) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Seek(TopicPartitionOffset tpo) => throw new NotImplementedException();
    public void SetSaslCredentials(string username, string password) => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.StoreOffset(ConsumeResult<byte[], byte[]> result) { }
    void IConsumer<byte[], byte[]>.StoreOffset(TopicPartitionOffset offset) { }

    void IConsumer<byte[], byte[]>.Subscribe(IEnumerable<string> topics)
    {
        ((IConsumer<byte[], byte[]>)this).Subscription.AddRange(topics);
        foreach (var topic in topics)
        {
            Streams.GetOrAdd(topic, t =>
                Enumerable.Range(0, 3)
                    .Select(n => new FakeKafkaMessageStream(new TopicPartition(t, n)))
                    .ToArray()
                );
        }

        ((IConsumer<byte[], byte[]>)this).Assign(Streams.Values.SelectMany(ts => ts.Select(s => s.TopicPartition)));
    }
    void IConsumer<byte[], byte[]>.Subscribe(string topic) => throw new NotImplementedException();

    void IConsumer<byte[], byte[]>.Unassign() => throw new NotImplementedException();
    void IConsumer<byte[], byte[]>.Unsubscribe()
    {
        foreach (var (_, ts) in Streams)
        {
            foreach (var s in ts)
                s.Stop();
        }
        ((IConsumer<byte[], byte[]>)this).Subscription.Clear();
    }


    void IProducer<byte[], byte[]>.AbortTransaction(TimeSpan timeout) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.AbortTransaction() => throw new NotImplementedException();
    int IClient.AddBrokers(string brokers) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.BeginTransaction() => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.CommitTransaction(TimeSpan timeout) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.CommitTransaction() => throw new NotImplementedException();
    void IDisposable.Dispose() => throw new NotImplementedException();
    int IProducer<byte[], byte[]>.Flush(TimeSpan timeout) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.Flush(CancellationToken cancellationToken) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.InitTransactions(TimeSpan timeout) => throw new NotImplementedException();
    int IProducer<byte[], byte[]>.Poll(TimeSpan timeout) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler) => throw new NotImplementedException();
    void IProducer<byte[], byte[]>.Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler) => throw new NotImplementedException();

    Task<DeliveryResult<byte[], byte[]>> IProducer<byte[], byte[]>.ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken)
    {
        var ts = Streams.GetOrAdd(topic, t => Enumerable.Range(0, 3)
                    .Select(n => new FakeKafkaMessageStream(new TopicPartition(t, n)))
                    .ToArray()
                );
        var partitionCount = ts.Length;
        var tp = ts
            .Skip((int)(Stopwatch.GetTimestamp() % partitionCount))
            .Select(i => i.TopicPartition)
            .First();

        return (this as IProducer<byte[], byte[]>).ProduceAsync(tp, message, cancellationToken);
    }

    Task<DeliveryResult<byte[], byte[]>> IProducer<byte[], byte[]>.ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken)
    {
        var ts = Streams.GetOrAdd(topicPartition.Topic, t => Enumerable.Range(0, 3)
                    .Select(n => new FakeKafkaMessageStream(new TopicPartition(t, n)))
                    .ToArray()
                );
        var stream = ts[topicPartition.Partition.Value];
        var tpo = stream.Write(message);
        var result = new DeliveryResult<byte[], byte[]>
        {
            Message = message,
            TopicPartitionOffset = tpo
        };
        ProducedMessages.Add(result);
        return Task.FromResult(result);
    }

    void IProducer<byte[], byte[]>.SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) => throw new NotImplementedException();
    void IClient.SetSaslCredentials(string username, string password) => throw new NotImplementedException();
}
