namespace CloudEventDotNet;

internal record class CloudEventSubscription(
    ICloudEventHandler Handler,
    SubscriptionOptions Options
    );
