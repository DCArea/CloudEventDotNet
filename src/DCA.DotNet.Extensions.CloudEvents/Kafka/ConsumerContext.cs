namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

public record ConsumerContext(string PubSubName, string Name, string Group);
