namespace IngestSvc.Storage;

public interface IPhotoUploader
{
    Task UploadAsync(string key, Stream fullRes, Stream lowRes, CancellationToken ct = default);
}
