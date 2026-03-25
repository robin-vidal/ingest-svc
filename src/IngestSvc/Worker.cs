using IngestSvc.Naming;
using IngestSvc.Watching;
using Microsoft.Extensions.Options;

namespace IngestSvc;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IOptions<WatcherOptions> _options;
    private readonly IFileSystemWatcherFactory _factory;
    private readonly IPhotoNamer _namer;

    public Worker(
        ILogger<Worker> logger,
        IOptions<WatcherOptions> options,
        IFileSystemWatcherFactory factory,
        IPhotoNamer namer)
    {
        _logger = logger;
        _options = options;
        _factory = factory;
        _namer = namer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var watcher = _factory.Create(_options.Value.Path);
        watcher.Filter = "*.jpg";
        watcher.NotifyFilter = NotifyFilters.FileName;
        watcher.Created += OnFileCreated;
        watcher.EnableRaisingEvents = true;

        _logger.LogInformation("Watching {Path}", _options.Value.Path);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        await WaitForFileReadyAsync(e.FullPath);
        var storageKey = _namer.Generate();
        _logger.LogInformation("Detected file: {Path} -> storage key: {Key}", e.FullPath, storageKey);
    }

    internal static async Task WaitForFileReadyAsync(
        string path,
        int maxAttempts = 10,
        int delayMs = 200)
    {
        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                using var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                return;
            }
            catch (IOException)
            {
                await Task.Delay(delayMs);
            }
        }

        // TODO: log a warning if still lock
    }
}
