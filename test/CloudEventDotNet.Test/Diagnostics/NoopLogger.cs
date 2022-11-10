using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Test.Diagnostics;
public partial class ActivityTests
{
    public class NoopLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) => throw new NotImplementedException();
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter){}
    }
}
