using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using IngestSvc.Watching;
using IngestSvc.Storage;
using IngestSvc.Naming;
using IngestSvc.Resizing;

namespace IngestSvc.Tests;

public class WorkerTests
{
    [Fact]
    public async Task WaitForFileReady_Returns_When_File_Size_Is_Stable()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllBytesAsync(path, new byte[1024]);

            await Worker.WaitForFileReadyAsync(path, maxAttempts: 3, delayMs: 50);
            // no exception = success
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task WaitForFileReady_Throws_When_File_Is_Empty()
    {
        var path = Path.GetTempFileName();
        try
        {
            await Assert.ThrowsAsync<TimeoutException>(
                () => Worker.WaitForFileReadyAsync(path, maxAttempts: 3, delayMs: 50));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ProcessFileAsync_OnSuccess_ShouldMoveToProcessedPath()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        
        var srcPath = Path.Combine(tempDir, "test.jpg");
        await File.WriteAllBytesAsync(srcPath, new byte[1024]);
        
        var processedDir = Path.Combine(tempDir, "processed");
        var failedDir = Path.Combine(tempDir, "failed");
        Directory.CreateDirectory(processedDir);
        Directory.CreateDirectory(failedDir);
        
        var loggerMock = new Mock<ILogger<Worker>>();
        var optionsMock = new Mock<IOptions<WatcherOptions>>();
        optionsMock.Setup(o => o.Value).Returns(new WatcherOptions
        {
            Path = tempDir,
            ProcessedPath = processedDir,
            FailedPath = failedDir
        });
        
        var factoryMock = new Mock<IFileSystemWatcherFactory>();
        var namerMock = new Mock<IPhotoNamer>();
        namerMock.Setup(n => n.Generate()).Returns("test-key");
        
        var resizerMock = new Mock<IPhotoResizer>();
        resizerMock.Setup(r => r.Resize(It.IsAny<Stream>())).Returns(new MemoryStream());
        
        var uploaderMock = new Mock<IPhotoUploader>();
        uploaderMock.Setup(u => u.UploadAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var worker = new Worker(
            loggerMock.Object,
            optionsMock.Object,
            factoryMock.Object,
            namerMock.Object,
            resizerMock.Object,
            uploaderMock.Object);
            
        await worker.ProcessFileAsync(srcPath);
        
        Assert.False(File.Exists(srcPath));
        Assert.True(File.Exists(Path.Combine(processedDir, "test.jpg")));
        
        Directory.Delete(tempDir, true);
    }

    [Fact]
    public async Task ProcessFileAsync_OnUploadFailure_ShouldMoveToFailedPath()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        
        var srcPath = Path.Combine(tempDir, "fail.jpg");
        await File.WriteAllBytesAsync(srcPath, new byte[1024]);
        
        var processedDir = Path.Combine(tempDir, "processed");
        var failedDir = Path.Combine(tempDir, "failed");
        Directory.CreateDirectory(processedDir);
        Directory.CreateDirectory(failedDir);
        
        var loggerMock = new Mock<ILogger<Worker>>();
        var optionsMock = new Mock<IOptions<WatcherOptions>>();
        optionsMock.Setup(o => o.Value).Returns(new WatcherOptions
        {
            Path = tempDir,
            ProcessedPath = processedDir,
            FailedPath = failedDir
        });
        
        var factoryMock = new Mock<IFileSystemWatcherFactory>();
        var namerMock = new Mock<IPhotoNamer>();
        var resizerMock = new Mock<IPhotoResizer>();
        resizerMock.Setup(r => r.Resize(It.IsAny<Stream>())).Returns(new MemoryStream());
        
        var uploaderMock = new Mock<IPhotoUploader>();
        uploaderMock.Setup(u => u.UploadAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Upload exploded"));
            
        var worker = new Worker(
            loggerMock.Object,
            optionsMock.Object,
            factoryMock.Object,
            namerMock.Object,
            resizerMock.Object,
            uploaderMock.Object);
            
        await worker.ProcessFileAsync(srcPath);
        
        Assert.False(File.Exists(srcPath));
        Assert.True(File.Exists(Path.Combine(failedDir, "fail.jpg")));
        
        Directory.Delete(tempDir, true);
    }

    [Fact]
    public async Task ProcessFileAsync_OnSuccess_MoveFails_ContinuesWithoutCrash()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        
        var srcPath = Path.Combine(tempDir, "movefail.jpg");
        await File.WriteAllBytesAsync(srcPath, new byte[1024]);
        
        var loggerMock = new Mock<ILogger<Worker>>();
        var optionsMock = new Mock<IOptions<WatcherOptions>>();
        optionsMock.Setup(o => o.Value).Returns(new WatcherOptions
        {
            Path = tempDir,
            ProcessedPath = "/invalid/path/that/does/not/exist/so/move/fails",
            FailedPath = "/invalid/failed"
        });
        
        var factoryMock = new Mock<IFileSystemWatcherFactory>();
        var namerMock = new Mock<IPhotoNamer>();
        var resizerMock = new Mock<IPhotoResizer>();
        resizerMock.Setup(r => r.Resize(It.IsAny<Stream>())).Returns(new MemoryStream());
        
        var uploaderMock = new Mock<IPhotoUploader>();
        uploaderMock.Setup(u => u.UploadAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var worker = new Worker(
            loggerMock.Object,
            optionsMock.Object,
            factoryMock.Object,
            namerMock.Object,
            resizerMock.Object,
            uploaderMock.Object);
            
        await worker.ProcessFileAsync(srcPath);
        
        Assert.True(File.Exists(srcPath));
        
        Directory.Delete(tempDir, true);
    }

    [Fact]
    public async Task Worker_GracefulShutdown_WaitsForOngoingProcessing()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        
        var processedDir = Path.Combine(tempDir, "processed");
        var failedDir = Path.Combine(tempDir, "failed");
        Directory.CreateDirectory(processedDir);
        Directory.CreateDirectory(failedDir);
        
        var srcPath = Path.Combine(tempDir, "ongoing.jpg");
        await File.WriteAllBytesAsync(srcPath, new byte[1024]);
        
        var loggerMock = new Mock<ILogger<Worker>>();
        var optionsMock = new Mock<IOptions<WatcherOptions>>();
        optionsMock.Setup(o => o.Value).Returns(new WatcherOptions { Path = tempDir, ProcessedPath = processedDir, FailedPath = failedDir });
        var factoryMock = new Mock<IFileSystemWatcherFactory>();
        var watcherMock = new FileSystemWatcher(tempDir);
        factoryMock.Setup(f => f.Create(It.IsAny<string>())).Returns(watcherMock);
        var namerMock = new Mock<IPhotoNamer>();
        var resizerMock = new Mock<IPhotoResizer>();
        resizerMock.Setup(r => r.Resize(It.IsAny<Stream>())).Returns(new MemoryStream());
        
        var uploaderMock = new Mock<IPhotoUploader>();
        var tcs = new TaskCompletionSource();
        bool uploadStarted = false;
        
        uploaderMock.Setup(u => u.UploadAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                uploadStarted = true;
                await tcs.Task; // Block upload indefinitely until we allow it
            });
            
        var worker = new Worker(loggerMock.Object, optionsMock.Object, factoryMock.Object, namerMock.Object, resizerMock.Object, uploaderMock.Object);
        
        var cts = new CancellationTokenSource();
        var workerTask = worker.StartAsync(cts.Token);
        
        try
        {
            // Wait till upload is definitely blocked
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (!uploadStarted && DateTime.UtcNow < timeout)
            {
                await Task.Delay(100);
            }
            
            Assert.True(uploadStarted, "Upload never started.");
            
            // Trigger SIGTERM
            var stopTask = worker.StopAsync(CancellationToken.None);
            
            // stopTask should not complete because uploader is hanging
            var delayWait = await Task.WhenAny(stopTask, Task.Delay(500));
            Assert.NotEqual(stopTask, delayWait); // stopTask is still hanging!
            
            // Complete the uploader
            tcs.SetResult();
            
            // Now stopTask should gracefully complete
            await stopTask;
            
            Assert.True(File.Exists(Path.Combine(processedDir, "ongoing.jpg")), "File was not moved to processed after wait.");
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            Directory.Delete(tempDir, true);
        }
    }
    
    [Fact]
    public async Task Worker_GracefulShutdown_CancelsPendingInQueue()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        var processedDir = Path.Combine(tempDir, "processed"); var failedDir = Path.Combine(tempDir, "failed");
        Directory.CreateDirectory(processedDir); Directory.CreateDirectory(failedDir);
        
        var loggerMock = new Mock<ILogger<Worker>>();
        var optionsMock = new Mock<IOptions<WatcherOptions>>();
        optionsMock.Setup(o => o.Value).Returns(new WatcherOptions { Path = tempDir, ProcessedPath = processedDir, FailedPath = failedDir });
        var factoryMock = new Mock<IFileSystemWatcherFactory>();
        var watcherMock = new FileSystemWatcher(tempDir);
        factoryMock.Setup(f => f.Create(It.IsAny<string>())).Returns(watcherMock);
        var namerMock = new Mock<IPhotoNamer>();
        var resizerMock = new Mock<IPhotoResizer>();
        resizerMock.Setup(r => r.Resize(It.IsAny<Stream>())).Returns(new MemoryStream());
        
        var uploaderMock = new Mock<IPhotoUploader>();
        var tcs = new TaskCompletionSource();
        int activeUploads = 0;
        
        uploaderMock.Setup(u => u.UploadAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                Interlocked.Increment(ref activeUploads);
                await tcs.Task; // block all
            });
            
        var worker = new Worker(loggerMock.Object, optionsMock.Object, factoryMock.Object, namerMock.Object, resizerMock.Object, uploaderMock.Object);
        var cts = new CancellationTokenSource();
        
        // Since maximum concurrency is 4 (SemaphoreSlim = 4)
        // Let's create 5 files BEFORE starting the worker
        for (int i = 0; i < 5; i++)
        {
            var p = Path.Combine(tempDir, $"file_{i}.jpg");
            await File.WriteAllBytesAsync(p, new byte[100]);
        }
        
        await worker.StartAsync(cts.Token);
        
        try
        {
            // Wait for 4 of them to hit the uploader (the 5th will be stuck in wait)
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (activeUploads < 4 && DateTime.UtcNow < timeout)
            {
                await Task.Delay(100);
            }
            
            Assert.Equal(4, activeUploads);
            
            // Stop service. The 5th task should be cancelled instantly!
            var stopTask = worker.StopAsync(CancellationToken.None);
            
            // Release the 4 workers
            tcs.SetResult();
            
            await stopTask;
            
            // The 5th should NEVER have started upload!
            Assert.Equal(4, activeUploads);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            Directory.Delete(tempDir, true);
        }
    }
}