namespace CloudEventDotNet;

internal record DeadLetterOptions(
    string PubSubName,
    string Source,
    string Topic);
