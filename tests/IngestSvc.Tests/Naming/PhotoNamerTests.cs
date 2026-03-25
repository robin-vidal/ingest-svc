using IngestSvc.Naming;
using IngestSvc.Watching;
using Microsoft.Extensions.Options;

namespace IngestSvc.Tests.Naming;

public class PhotoNamerTests
{
    private static IPhotoNamer CreateNamer(string standId = "stand42") =>
        new PhotoNamer(Options.Create(new WatcherOptions { StandId = standId }));

    [Fact]
    public void Generate_ReturnsExpectedFormat()
    {
        var namer = CreateNamer("mystand");

        var key = namer.Generate();

        // Expected: "mystand-{uuid}.jpg"
        Assert.Matches(@"^mystand-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.jpg$", key);
    }

    [Fact]
    public void Generate_NeverUsesOriginalFilename()
    {
        var namer = CreateNamer("stand1");
        const string originalFilename = "IMG_1234.jpg";

        var key = namer.Generate();

        Assert.DoesNotContain(originalFilename, key);
    }

    [Fact]
    public void Generate_ProducesUniqueKeys()
    {
        var namer = CreateNamer();
        const int count = 1000;

        var keys = Enumerable.Range(0, count).Select(_ => namer.Generate()).ToList();

        Assert.Equal(count, keys.Distinct().Count());
    }

    [Fact]
    public async Task Generate_ProducesUniqueKeys_UnderConcurrency()
    {
        var namer = CreateNamer();
        const int concurrency = 100;
        var results = new System.Collections.Concurrent.ConcurrentBag<string>();

        await Task.WhenAll(
            Enumerable.Range(0, concurrency).Select(_ => Task.Run(() => results.Add(namer.Generate())))
        );

        Assert.Equal(concurrency, results.Distinct().Count());
    }

    [Fact]
    public void Generate_IncludesStandIdPrefix()
    {
        var namer = CreateNamer("stand99");

        var key = namer.Generate();

        Assert.StartsWith("stand99-", key);
    }
}