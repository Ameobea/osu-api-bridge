using System.Collections.Concurrent;
using System.Diagnostics;
using DiffCalc.Configuration;
using DiffCalc.Observability;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace DiffCalc.Beatmaps;

public sealed class CachedBeatmapProvider
{
    private readonly BeatmapStore store;
    private readonly MemoryCache cache;
    private readonly ConcurrentDictionary<int, Lazy<Task<MemoryWorkingBeatmap>>> loads = new();
    private long estimatedBytes;
    private int entries;

    public CachedBeatmapProvider(BeatmapStore store, IOptions<DiffCalcOptions> options)
    {
        this.store = store;
        cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = options.Value.BeatmapCacheSizeBytes });
    }

    public async Task<MemoryWorkingBeatmap> GetAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(beatmapId, out MemoryWorkingBeatmap? cached) && cached is not null)
        {
            DiffCalcMetrics.BeatmapCacheAccess.WithLabels(operation, "hit").Inc();
            return cached;
        }

        DiffCalcMetrics.BeatmapCacheAccess.WithLabels(operation, "miss").Inc();
        var load = loads.GetOrAdd(
            beatmapId,
            id => new Lazy<Task<MemoryWorkingBeatmap>>(
                () => LoadAsync(id, operation, CancellationToken.None),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return await load.Value.WaitAsync(cancellationToken);
        }
        finally
        {
            if (load.IsValueCreated && load.Value.IsCompleted)
                loads.TryRemove(new KeyValuePair<int, Lazy<Task<MemoryWorkingBeatmap>>>(beatmapId, load));
        }
    }

    private async Task<MemoryWorkingBeatmap> LoadAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        var raw = await store.GetRawBeatmapAsync(beatmapId, operation, cancellationToken);
        var stopwatch = Stopwatch.StartNew();
        MemoryWorkingBeatmap working;
        try
        {
            working = new MemoryWorkingBeatmap(raw, beatmapId);
        }
        finally
        {
            DiffCalcMetrics.BeatmapParseDuration.WithLabels(operation).Observe(stopwatch.Elapsed.TotalSeconds);
        }

        var estimatedSize = Math.Max(64 * 1024L, raw.LongLength * 8);
        cache.Set(
            beatmapId,
            working,
            new MemoryCacheEntryOptions()
                .SetSize(estimatedSize)
                .RegisterPostEvictionCallback((_, _, _, state) => ((CachedBeatmapProvider)state!).OnEvicted(estimatedSize), this));
        Interlocked.Add(ref estimatedBytes, estimatedSize);
        Interlocked.Increment(ref entries);
        PublishGauges();
        return working;
    }

    private void OnEvicted(long size)
    {
        Interlocked.Add(ref estimatedBytes, -size);
        Interlocked.Decrement(ref entries);
        PublishGauges();
    }

    private void PublishGauges()
    {
        DiffCalcMetrics.BeatmapCacheEstimatedBytes.Set(Interlocked.Read(ref estimatedBytes));
        DiffCalcMetrics.BeatmapCacheEntries.Set(Volatile.Read(ref entries));
    }
}
