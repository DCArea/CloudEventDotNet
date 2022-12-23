using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    IDatabase Redis,
    RedisMessageTelemetry RedisTelemetry);
