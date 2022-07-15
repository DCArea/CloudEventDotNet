using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DCA.DotNet.Extensions.CloudEvents;

public class SubscribeHostedService : IHostedService
{
    private readonly PubSubOptions _options;
    private readonly Dictionary<string, ICloudEventSubscriber> _subscribers;
    private readonly ILogger<SubscribeHostedService> _logger;
    private Task? _task;
    private CancellationTokenSource? _stoppingCts;

    public SubscribeHostedService(
        ILogger<SubscribeHostedService> logger,
        IServiceProvider serviceProvider,
        IOptions<PubSubOptions> options)
    {
        _options = options.Value;
        _subscribers = _options.SubscriberFactoris
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value(serviceProvider));
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting subscribers");
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var tasks = new List<Task>();
        foreach (var (pubsub, subscriber) in _subscribers)
        {
            var task = Task.Factory.StartNew(
                () => Subscribe(subscriber, _stoppingCts.Token),
                _stoppingCts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            tasks.Add(task);
        }
        _task = Task.WhenAll(tasks);
        _logger.LogInformation("Started subscribers");
        return Task.CompletedTask;
    }

    private void Subscribe(ICloudEventSubscriber subscriber, CancellationToken cancellationToken)
    {
        try
        {
            subscriber.Subscribe(cancellationToken);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error on subscribing");
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stoping subscribers");
        if (_task == null)
        {
            return;
        }

        try
        {
            _stoppingCts!.Cancel();
        }
        finally
        {
            await Task.WhenAny(_task, Task.Delay(Timeout.Infinite, cancellationToken)).ConfigureAwait(false);
            _logger.LogInformation("Stopped subscribers");
        }
    }
}
