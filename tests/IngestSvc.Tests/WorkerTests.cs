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
}