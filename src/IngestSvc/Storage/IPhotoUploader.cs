namespace IngestSvc.Storage;

public interface IPhotoUploader
{
    Task EnsureReadyAsync(CancellationToken ct = default);
    Task UploadAsync(string key, Stream fullRes, Stream lowRes, CancellationToken ct = default);
}
