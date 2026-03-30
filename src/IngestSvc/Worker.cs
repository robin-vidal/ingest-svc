using IngestSvc.Naming;
using IngestSvc.Resizing;
using IngestSvc.Storage;
using IngestSvc.Watching;
using Microsoft.Extensions.Options;

namespace IngestSvc;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IOptions<WatcherOptions> _options;
    private readonly IFileSystemWatcherFactory _factory;
    private readonly IPhotoNamer _namer;
    private readonly IPhotoResizer _resizer;
    private readonly IPhotoUploader _uploader;
    private readonly SemaphoreSlim _semaphore = new(4);

    public Worker(
        ILogger<Worker> logger,
        IOptions<WatcherOptions> options,
        IFileSystemWatcherFactory factory,
        IPhotoNamer namer,
        IPhotoResizer resizer,
        IPhotoUploader uploader)
    {
        _logger = logger;
        _options = options;
        _factory = factory;
        _namer = namer;
        _resizer = resizer;
        _uploader = uploader;
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
        var key = _namer.Generate();
        _logger.LogInformation("Detected {Path} -> key {Key}", e.FullPath, key);

        await _semaphore.WaitAsync();
        try
        {
            using var file = File.OpenRead(e.FullPath);
            using var fullRes = new MemoryStream();
            await file.CopyToAsync(fullRes);

            using var forResize = new MemoryStream(fullRes.ToArray());
            using var lowRes = _resizer.Resize(forResize);

            fullRes.Seek(0, SeekOrigin.Begin);
            await _uploader.UploadAsync(key, fullRes, lowRes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process {Path}", e.FullPath);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    internal static async Task WaitForFileReadyAsync(
        string path,
        int maxAttempts = 30,
        int delayMs = 500)
    {
        long previousSize = -1;

        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                long currentSize = new FileInfo(path).Length;

                if (currentSize > 0 && currentSize == previousSize)
                    return;

                previousSize = currentSize;
            }
            catch (IOException)
            {
                // File not yet accessible
            }

            await Task.Delay(delayMs);
        }

        throw new TimeoutException(
            $"File not ready after {maxAttempts * delayMs / 1000}s: {path}");
    }
}
