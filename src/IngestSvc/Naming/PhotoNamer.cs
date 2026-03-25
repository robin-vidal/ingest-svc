using IngestSvc.Watching;
using Microsoft.Extensions.Options;

namespace IngestSvc.Naming;

public sealed class PhotoNamer : IPhotoNamer
{
    private readonly string _standId;

    public PhotoNamer(IOptions<WatcherOptions> options)
    {
        _standId = options.Value.StandId;
    }

    public string Generate() => $"{_standId}-{Guid.NewGuid()}.jpg";
}