using IngestSvc;
using IngestSvc.Naming;
using IngestSvc.Resizing;
using IngestSvc.Storage;
using IngestSvc.Watching;
using Minio;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.Configure<WatcherOptions>(
    builder.Configuration.GetSection("Watcher")
);
builder.Services.Configure<ResizeOptions>(
    builder.Configuration.GetSection("Resize")
);
builder.Services.Configure<StorageOptions>(
    builder.Configuration.GetSection("Storage")
);
builder.Services.AddSingleton<IFileSystemWatcherFactory, FileSystemWatcherFactory>();
builder.Services.AddSingleton<IPhotoNamer, PhotoNamer>();
builder.Services.AddSingleton<IPhotoResizer, PhotoResizer>();
builder.Services.AddSingleton<IPhotoUploader, PhotoUploader>();

var storageConfig = builder.Configuration.GetSection("Storage").Get<StorageOptions>()
    ?? throw new InvalidOperationException("Storage configuration is required");

if (string.IsNullOrWhiteSpace(storageConfig.Endpoint) || 
    string.IsNullOrWhiteSpace(storageConfig.AccessKey) || 
    string.IsNullOrWhiteSpace(storageConfig.SecretKey))
{
    throw new InvalidOperationException("Storage configuration requires Endpoint, AccessKey, and SecretKey to be non-empty.");
}

var watcherConfig = builder.Configuration.GetSection("Watcher").Get<WatcherOptions>()
    ?? throw new InvalidOperationException("Watcher configuration is required");

if (string.IsNullOrWhiteSpace(watcherConfig.Path) ||
    string.IsNullOrWhiteSpace(watcherConfig.ProcessedPath) ||
    string.IsNullOrWhiteSpace(watcherConfig.FailedPath))
{
    throw new InvalidOperationException("Watcher configuration requires Path, ProcessedPath, and FailedPath to be non-empty.");
}

builder.Services.Configure<HostOptions>(options =>
{
    options.ShutdownTimeout = TimeSpan.FromMinutes(5);
    options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
});

builder.Services.AddMinio(configureClient => configureClient
    .WithEndpoint(storageConfig.Endpoint)
    .WithCredentials(storageConfig.AccessKey, storageConfig.SecretKey)
    .WithSSL(storageConfig.UseSSL)
    .Build());

var host = builder.Build();
host.Run();
