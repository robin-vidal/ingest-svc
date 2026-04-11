using System.Net;
using IngestSvc.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel.Args;
using Minio.DataModel.Response;
using Minio.Exceptions;
using Moq;

namespace IngestSvc.Tests.Storage;

public class PhotoUploaderTests
{
    private static IPhotoUploader CreateUploader(IMinioClient client, string bucket = "photos", string fullPrefix = "full", string lowPrefix = "low", int retryInit = 10, int retryMax = 50) =>
        new PhotoUploader(
            client,
            Options.Create(new StorageOptions
            {
                Bucket = bucket,
                FullPrefix = fullPrefix,
                LowPrefix = lowPrefix,
                RetryInitialDelayMs = retryInit,
                RetryMaxDelayMs = retryMax
            }),
            NullLogger<PhotoUploader>.Instance
        );

    private static PutObjectResponse OkResponse() =>
        new(HttpStatusCode.OK, string.Empty, new Dictionary<string, string>(), 0, string.Empty);

    [Fact]
    public async Task EnsureReadyAsync_Succeeds_When_BucketExists()
    {
        var mock = new Mock<IMinioClient>();
        mock.Setup(c => c.BucketExistsAsync(It.IsAny<BucketExistsArgs>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var uploader = CreateUploader(mock.Object);
        await uploader.EnsureReadyAsync();
        
        mock.Verify(c => c.BucketExistsAsync(It.IsAny<BucketExistsArgs>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EnsureReadyAsync_Retries_On_TransientNetworkError()
    {
        var callCount = 0;
        var mock = new Mock<IMinioClient>();
        mock.Setup(c => c.BucketExistsAsync(It.IsAny<BucketExistsArgs>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount < 3)
                    throw new HttpRequestException("Network down");
                return Task.FromResult(true);
            });

        var uploader = CreateUploader(mock.Object);
        await uploader.EnsureReadyAsync();
        
        Assert.Equal(3, callCount);
    }

    [Fact]
    public async Task EnsureReadyAsync_Retries_When_BucketDoesNotExist()
    {
        var mock = new Mock<IMinioClient>();
        mock.Setup(c => c.BucketExistsAsync(It.IsAny<BucketExistsArgs>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var uploader = CreateUploader(mock.Object, retryInit: 1, retryMax: 1);
        
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<Exception>(() => uploader.EnsureReadyAsync(cts.Token));
    }

    [Fact]
    public async Task UploadAsync_Retries_On_TransientError()
    {
        var callCount = 0;
        var mock = new Mock<IMinioClient>();
        mock.Setup(c => c.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount < 3)
                    throw new MinioException("connection refused"); 
                return Task.FromResult(OkResponse());
            });

        var uploader = CreateUploader(mock.Object, retryInit: 10, retryMax: 50);
        
        using var fullRes = new MemoryStream(new byte[10]);
        using var lowRes = new MemoryStream(new byte[5]);
        
        await uploader.UploadAsync("test.jpg", fullRes, lowRes);
        
        Assert.Equal(4, callCount); // 2 misses, 1 hit, plus 1 hit for low res
    }

    [Fact]
    public async Task UploadAsync_ThrowsImmediately_On_AuthorizationException()
    {
        var mock = new Mock<IMinioClient>();
        mock.Setup(c => c.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AuthorizationException());

        var uploader = CreateUploader(mock.Object);
        
        using var fullRes = new MemoryStream(new byte[10]);
        using var lowRes = new MemoryStream(new byte[5]);
        
        await Assert.ThrowsAsync<AuthorizationException>(() => uploader.UploadAsync("test.jpg", fullRes, lowRes));
    }
}
