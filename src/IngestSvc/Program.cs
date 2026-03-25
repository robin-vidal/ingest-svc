using IngestSvc;
using IngestSvc.Naming;
using IngestSvc.Watching;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.Configure<WatcherOptions>(
    builder.Configuration.GetSection("Watcher")
);
builder.Services.AddSingleton<IFileSystemWatcherFactory, FileSystemWatcherFactory>();
builder.Services.AddSingleton<IPhotoNamer, PhotoNamer>();

var host = builder.Build();
host.Run();
