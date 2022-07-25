using System.Threading.Tasks.Sources;

namespace CloudEventDotNet;

internal class WorkItemWaiter : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> _tcs;
    public void GetResult(short token) => _tcs.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _tcs.GetStatus(token);
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);

    public void SetResult() => _tcs.SetResult(true);
    public ValueTask Task => new(this, _tcs.Version);
}
