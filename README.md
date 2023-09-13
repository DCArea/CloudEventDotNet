# CloudEventDotNet

Publish/Subscribe CloudEvents in .NET, inspired by Dapr Pub/Sub Component

## Features

* Publish/Subscribe events with CloudEvent format
* With both Kafka and Redis support
* At-Least-Once delivery guarantee
* Redeliver failed events
* Obeservability support (traces/metrics)
* ***DOES NOT*** support delivery events in order

## Usage

### Install package
```shell
dotnet add package CloudEventDotNet
```

### Configure pubsub:
```csharp
services.AddCloudEvents(defaultPubSubName: "kafka", defaultTopic: "my-topic")
    .Load(typeof(OrderCancelled).Assembly)
    .AddKafkaPubSub("kafka", options =>
    {
        options.ProducerConfig = new ProducerConfig
        {
            BootstrapServers = broker,
        };
    }, options =>
    {
        options.ConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = consumerGroup,
        };
    })
    .AddRedisPubSub("redis", options =>
    {
        options.ConnectionMultiplexerFactory = () => redis;
        options.MaxLength = maxLength;
    }, options =>
    {
        options.ConnectionMultiplexerFactory = () => redis;
        options.ConsumerGroup = consumerGroup;
    });
```

#### Define cloud event:
```csharp
[CloudEvent] // register event with default pubsub name and topic
public record OrderCancelled(Guid OrderId, string Reason);
```

#### Define cloud event with custom metadata
```csharp
[CloudEvent(PubSubName = "redis", Topic = "another-topic", Type = "a-custom-type")]
public record OrderCancelled(Guid OrderId, string Reason);
```

### Publish a cloud event:
```csharp
var pubsub = serviceProvider.GetRequiredService<ICloudEventPubSub>();
await pubsub.PublishAsync(new OrderCancelled(order.Id, reason));
```

### Subscribe and process cloud event:
``` csharp
public sealed class OrderCancelledHandler : ICloudEventHandler<OrderCancelled>
{
    public async Task HandleAsync(CloudEvent<PingEvent> cloudEvent, CancellationToken token)
    {
        // ...
    }
}
```

## Performance

The benchmark result on a 4*2.4GHz Core VM:

|      | Kafka | Redis |  
| --- | --- | --- |  
| Publish | ~100k/s | ~90k/s |  
| Subscribe | ~150k/s | ~40k/s |  

The benchmark code is located at `perf/CloudEventTester`
