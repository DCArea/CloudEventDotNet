namespace CloudEventKafkaTester
{
    internal static class RedisEnv
    {
        public static string redisConnectionString { get; set; } = Environment.GetEnvironmentVariable("CONNSTR") ?? "localhost:6379";
        public static string topic { get; set; } = Environment.GetEnvironmentVariable("TOPIC") ?? "devperftest";
        public static string consumerGroup { get; set; } = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "devperftest";
        public static int runningWorkItemLimit { get; set; } = int.Parse(Environment.GetEnvironmentVariable("RUNNING_WORK_ITEM_LIMIT") ?? "1024");
        public static int maxLength { get; set; } = int.Parse(Environment.GetEnvironmentVariable("MAX_LENGTH") ?? (100_000_000).ToString());
    }
}
