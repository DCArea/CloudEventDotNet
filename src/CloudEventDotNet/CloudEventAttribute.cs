﻿namespace CloudEventDotNet;

/// <summary>
/// CloudEvent atribute
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CloudEventAttribute : Attribute
{
    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#type">'type'</see> attribute.
    /// Type of occurrence which has happened.
    /// Often this attribute is used for routing, observability, policy enforcement, etc.
    /// </summary>
    public string? Type { get; init; }

    /// <summary>
    /// CloudEvents <see href="https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md#source">'source'</see> attribute.
    /// This describes the event producer. Often this will include information such as the type of the event source, the
    /// organization publishing the event, the process that produced the event, and some unique identifiers.
    /// </summary>
    /// <remarks>
    /// Default to use the defaultSource configured on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? Source { get; init; }

    /// <summary>
    /// The PubSub's name to which CloudEvents will be sent
    /// </summary>
    /// <remarks>
    /// Default to use the defaultPubSubName configured on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? PubSubName { get; init; }

    /// <summary>
    /// The topic to which CloudEvents will be sent.
    /// </summary>
    /// <remarks>
    /// Default to use the defaultTopic configured on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? Topic { get; init; }

    /// <summary>
    /// Whether to enable deadletter, if enabled, a deadletter will be sent when this CloudEvent fails to be handled.
    /// </summary>
    /// <remarks>
    /// Default to configuration on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public bool? EnableDeadLetter { get; init; }

    /// <summary>
    /// The name of the PubSub to which the DeadLetter will be sent.
    /// </summary>
    /// <remarks>
    /// Default to configuration on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? DeadLetterPubSubName { get; init; }

    /// <summary>
    /// The source to which the DeadLetter will be sent.
    /// </summary>
    /// <remarks>
    /// Default to configuration on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? DeadLetterSource { get; init; }

    /// <summary>
    /// The topic to which the DeadLetter will be sent.
    /// </summary>
    /// <remarks>
    /// Default to configuration on <see cref="PubSubBuilder"/>.
    /// </remarks>
    public string? DeadLetterTopic { get; init; }
}
