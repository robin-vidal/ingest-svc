using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel.Args;

namespace IngestSvc.Storage;

public sealed class PhotoUploader : IPhotoUploader
{
    private readonly IMinioClient _client;
    private readonly StorageOptions _options;
    private readonly ILogger<PhotoUploader> _logger;

    public PhotoUploader(
        IMinioClient client,
        IOptions<StorageOptions> options,
        ILogger<PhotoUploader> logger)
    {
        _client = client;
        _options = options.Value;
        _logger = logger;
    }

    public async Task UploadAsync(string key, Stream fullRes, Stream lowRes, CancellationToken ct = default)
    {
        await UploadOneAsync($"{_options.FullPrefix}/{key}", fullRes, ct);
        await UploadOneAsync($"{_options.LowPrefix}/{key}", lowRes, ct);
    }

    private async Task UploadOneAsync(string objectName, Stream stream, CancellationToken ct)
    {
        var args = new PutObjectArgs()
            .WithBucket(_options.Bucket)
            .WithObject(objectName)
            .WithStreamData(stream)
            .WithObjectSize(stream.Length)
            .WithContentType("image/jpeg");

        await _client.PutObjectAsync(args, ct);
        _logger.LogInformation("Uploaded {Object} to {Bucket}", objectName, _options.Bucket);
    }
}
