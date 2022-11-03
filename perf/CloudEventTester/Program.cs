using CloudEventTester;

Tester tester = args[0] switch
{
    "ping" => new PingTester(),
    "serialization" => new SerializationTester(),
    _ => throw new ArgumentOutOfRangeException("target")
};

await tester.RunAsync(args);
