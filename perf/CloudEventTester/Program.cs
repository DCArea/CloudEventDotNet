using CloudEventTester;

#pragma warning disable CA2208 // Instantiate argument exceptions correctly
Tester tester = args[0] switch
{
    "ping" => new PingTester(),
    "serialization" => new SerializationTester(),
    "dl" => new DeadLetterTester(),
    _ => throw new ArgumentOutOfRangeException("target")
};

await tester.RunAsync(args);
