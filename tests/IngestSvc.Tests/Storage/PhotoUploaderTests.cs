using System.Net;
using System.Reflection;
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
    private static IPhotoUploader CreateUploader(IMinioClient client, string bucket = "photos", string fullPrefix = "full", string lowPrefix = "low") =>
        new PhotoUploader(
            client,
            Options.Create(new StorageOptions
            {
                Bucket = bucket,
                FullPrefix = fullPrefix,
                LowPrefix = lowPrefix
            }),
            NullLogger<PhotoUploader>.Instance
        );

    private static PutObjectResponse OkResponse() =>
        new(HttpStatusCode.OK, string.Empty, new Dictionary<string, string>(), 0, string.Empty);

    private static string GetObjectName(PutObjectArgs args)
    {
        var type = args.GetType().BaseType;
        while (type != null && type.Name != "ObjectArgs`1")
            type = type.BaseType;
        return (string)type!.GetProperty("ObjectName", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(args)!;
    }

    [Fact]
    public async Task UploadAsync_UploadsBothVersionsWithCorrectPaths()
    {
        var capturedArgs = new List<PutObjectArgs>();
        var mock = new Mock<IMinioClient>();
        mock.Setup(m => m.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()))
            .Callback<PutObjectArgs, CancellationToken>((a, _) => capturedArgs.Add(a))
            .ReturnsAsync(OkResponse());

        var uploader = CreateUploader(mock.Object);
        using var fullRes = new MemoryStream(new byte[10]);
        using var lowRes = new MemoryStream(new byte[5]);

        await uploader.UploadAsync("stand-1-abc.jpg", fullRes, lowRes);

        Assert.Equal(2, capturedArgs.Count);
        Assert.Equal("full/stand-1-abc.jpg", GetObjectName(capturedArgs[0]));
        Assert.Equal("low/stand-1-abc.jpg", GetObjectName(capturedArgs[1]));
    }

    [Fact]
    public async Task UploadAsync_FullResFailure_ThrowsAndSkipsLowRes()
    {
        var mock = new Mock<IMinioClient>();
        mock.Setup(m => m.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MinioException("connection refused"));

        var uploader = CreateUploader(mock.Object);
        using var fullRes = new MemoryStream(new byte[10]);
        using var lowRes = new MemoryStream(new byte[5]);

        await Assert.ThrowsAsync<MinioException>(() =>
            uploader.UploadAsync("stand-1-abc.jpg", fullRes, lowRes));

        mock.Verify(m => m.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadAsync_LowResFailure_Throws()
    {
        var mock = new Mock<IMinioClient>();
        mock.SetupSequence(m => m.PutObjectAsync(It.IsAny<PutObjectArgs>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OkResponse())
            .ThrowsAsync(new MinioException("low-res upload failed"));

        var uploader = CreateUploader(mock.Object);
        using var fullRes = new MemoryStream(new byte[10]);
        using var lowRes = new MemoryStream(new byte[5]);

        await Assert.ThrowsAsync<MinioException>(() =>
            uploader.UploadAsync("stand-1-abc.jpg", fullRes, lowRes));
    }
}
