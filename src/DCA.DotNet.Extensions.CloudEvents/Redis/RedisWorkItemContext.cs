using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    IDatabase Redis,
    RedisMessageTelemetry RedisTelemetry);
