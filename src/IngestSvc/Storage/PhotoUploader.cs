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

    public async Task EnsureReadyAsync(CancellationToken ct = default)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            var args = new BucketExistsArgs().WithBucket(_options.Bucket);
            bool exists = await _client.BucketExistsAsync(args, ct);
            if (!exists)
            {
                _logger.LogWarning("Bucket {Bucket} does not exist yet. Please create it.", _options.Bucket);
                // Throw an exception so it can be retried until created, but we use a specific transient-like exception.
                throw new InvalidOperationException($"Bucket '{_options.Bucket}' missing.");
            }
        }, ct);
        _logger.LogInformation("MinIO bucket {Bucket} is ready.", _options.Bucket);
    }

    public async Task UploadAsync(string key, Stream fullRes, Stream lowRes, CancellationToken ct = default)
    {
        await UploadOneAsync($"{_options.FullPrefix}/{key}", fullRes, ct);
        await UploadOneAsync($"{_options.LowPrefix}/{key}", lowRes, ct);
    }

    private async Task UploadOneAsync(string objectName, Stream stream, CancellationToken ct)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            var args = new PutObjectArgs()
                .WithBucket(_options.Bucket)
                .WithObject(objectName)
                .WithStreamData(stream)
                .WithObjectSize(stream.Length)
                .WithContentType("image/jpeg");

            await _client.PutObjectAsync(args, ct);
        }, ct);
        
        _logger.LogInformation("Uploaded {Object} to {Bucket}", objectName, _options.Bucket);
    }

    private async Task ExecuteWithRetryAsync(Func<Task> action, CancellationToken ct)
    {
        int delayMs = _options.RetryInitialDelayMs;
        int attempt = 0;

        while (true)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsRetriable(ex))
            {
                attempt++;
                _logger.LogWarning(ex, "MinIO operation failed (attempt {Attempt}). Retrying in {Delay}ms...", attempt, delayMs);
                await Task.Delay(delayMs, ct);
                
                delayMs = Math.Min(delayMs * 2, _options.RetryMaxDelayMs);
            }
        }
    }

    private static bool IsRetriable(Exception ex)
    {
        if (ex is HttpRequestException || ex is TimeoutException || ex is IOException || ex is InvalidOperationException)
            return true;

        if (ex is Minio.Exceptions.MinioException minioEx)
        {
            var name = ex.GetType().Name;
            // Filter out purely non-transient client errors
            if (name == "AuthorizationException" || 
                name == "InvalidBucketNameException" ||
                name == "AccessDeniedException")
            {
                return false;
            }
            return true;
        }

        return false;
    }
}
