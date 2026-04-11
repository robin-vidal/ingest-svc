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
        if (!string.IsNullOrWhiteSpace(_options.Value.ProcessedPath))
        {
            Directory.CreateDirectory(_options.Value.ProcessedPath);
        }

        if (!string.IsNullOrWhiteSpace(_options.Value.FailedPath))
        {
            Directory.CreateDirectory(_options.Value.FailedPath);
        }

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
        try
        {
            await ProcessFileAsync(e.FullPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception while handling created event for {Path}", e.FullPath);
        }
    }

    internal async Task ProcessFileAsync(string fullPath)
    {
        await WaitForFileReadyAsync(fullPath);
        var key = _namer.Generate();
        _logger.LogInformation("Detected {Path} -> key {Key}", fullPath, key);

        bool uploadSuccess = false;

        await _semaphore.WaitAsync();
        try
        {
            using (var file = File.OpenRead(fullPath))
            {
                using var fullRes = new MemoryStream();
                await file.CopyToAsync(fullRes);

                using var forResize = new MemoryStream(fullRes.ToArray());
                using var lowRes = _resizer.Resize(forResize);

                fullRes.Seek(0, SeekOrigin.Begin);
                await _uploader.UploadAsync(key, fullRes, lowRes);
                uploadSuccess = true;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process {Path}", fullPath);
        }
        finally
        {
            _semaphore.Release();
        }

        var destFolder = uploadSuccess ? _options.Value.ProcessedPath : _options.Value.FailedPath;

        if (string.IsNullOrWhiteSpace(destFolder))
        {
            return;
        }

        try
        {
            var destPath = Path.Combine(destFolder, Path.GetFileName(fullPath));
            File.Move(fullPath, destPath, overwrite: true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move file {Path} to {Destination}", fullPath, destFolder);
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
