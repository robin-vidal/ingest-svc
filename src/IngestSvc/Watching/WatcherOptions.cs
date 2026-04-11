namespace IngestSvc.Watching;

public sealed class WatcherOptions
{
    public string Path { get; set; } = string.Empty;
    public string StandId { get; set; } = string.Empty;
    public string ProcessedPath { get; set; } = string.Empty;
    public string FailedPath { get; set; } = string.Empty;
}