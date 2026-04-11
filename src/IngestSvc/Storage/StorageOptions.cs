namespace IngestSvc.Storage;

public sealed class StorageOptions
{
    public string Endpoint { get; set; } = string.Empty;
    public string AccessKey { get; set; } = string.Empty;
    public string SecretKey { get; set; } = string.Empty;
    public bool UseSSL { get; set; } = false;
    public string Bucket { get; set; } = string.Empty;
    public string FullPrefix { get; set; } = string.Empty;
    public string LowPrefix { get; set; } = string.Empty;
    public int RetryInitialDelayMs { get; set; } = 1000;
    public int RetryMaxDelayMs { get; set; } = 60000;
}
