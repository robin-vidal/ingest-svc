using System.Collections.Concurrent;
using IngestSvc.Naming;
using IngestSvc.Resizing;
using IngestSvc.Storage;
using IngestSvc.Watching;
using Microsoft.Extensions.Options;

namespace IngestSvc;

public class Worker : BackgroundService
{
    private readonly ConcurrentDictionary<string, byte> _processingFiles = new();
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

        _logger.LogInformation("Waiting for MinIO to become ready...");
        try 
        {
            await _uploader.EnsureReadyAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to ensure MinIO readiness. Shutting down worker.");
            throw;
        }

        using var watcher = _factory.Create(_options.Value.Path);
        watcher.Filter = "*.jpg";
        watcher.NotifyFilter = NotifyFilters.FileName;
        watcher.InternalBufferSize = 65536; // Handle huge bursts up to ~1000 files
        watcher.Created += OnFileCreated;
        watcher.EnableRaisingEvents = true;

        _logger.LogInformation("Watching {Path}", _options.Value.Path);

        // Sweep periodically in the background for any missed events or timeouts
        _ = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
            SweepDirectory();

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                SweepDirectory();
            }
        }, stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private void SweepDirectory()
    {
        try
        {
            foreach (var existingFile in Directory.GetFiles(_options.Value.Path, "*.jpg"))
            {
                _ = Task.Run(() => ProcessSafeAsync(existingFile));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to perform directory sweep.");
        }
    }

    private void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        _ = Task.Run(() => ProcessSafeAsync(e.FullPath));
    }

    private async Task ProcessSafeAsync(string fullPath)
    {
        if (!_processingFiles.TryAdd(fullPath, 0))
            return; // Already being processed by sweep or watcher

        try
        {
            await ProcessFileAsync(fullPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error on {Path}", fullPath);
        }
        finally
        {
            _processingFiles.TryRemove(fullPath, out _);
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
        int maxAttempts = 240, // 2 minutes timeout to support massive parallel copying
        int delayMs = 500)
    {
        long previousSize = -1;

        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                long currentSize = new FileInfo(path).Length;

                if (currentSize > 0 && currentSize == previousSize)
                {
                    using var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                    return;
                }

                previousSize = currentSize;
            }
            catch (IOException)
            {
                // File not yet accessible
            }
            catch (UnauthorizedAccessException)
            {
                // File permissions being set
            }

            await Task.Delay(delayMs);
        }

        throw new TimeoutException(
            $"File not ready after {maxAttempts * delayMs / 1000}s: {path}");
    }
}
