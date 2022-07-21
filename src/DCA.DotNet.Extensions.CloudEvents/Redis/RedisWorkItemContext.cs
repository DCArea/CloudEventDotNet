using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal record RedisWorkItemContext(
    Registry Registry,
    IServiceScopeFactory ScopeFactory,
    ILogger Logger,
    IDatabase Redis,
    RedisMessageTelemetry RedisTelemetry);
