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
    private readonly ILogger<CachedBeatmapProvider> logger;
    private readonly MemoryCache cache;
    private readonly ConcurrentDictionary<int, Lazy<Task<MemoryWorkingBeatmap>>> loads = new();
    private long estimatedBytes;
    private int entries;

    public CachedBeatmapProvider(BeatmapStore store, IOptions<DiffCalcOptions> options, ILogger<CachedBeatmapProvider> logger)
    {
        this.store = store;
        this.logger = logger;
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

    /// <summary>
    /// Loads every uncached beatmap of a batch with one database query and returns them: a full cache rejects new
    /// entries, so the batch must hold its own references. Beatmaps missing from the database, and any failure here,
    /// are left to the per-beatmap path in <see cref="GetAsync"/>.
    /// </summary>
    public async Task<IReadOnlyDictionary<int, MemoryWorkingBeatmap>> PrefetchAsync(
        IEnumerable<int> beatmapIds,
        string operation,
        CancellationToken cancellationToken)
    {
        var loaded = new ConcurrentDictionary<int, MemoryWorkingBeatmap>();
        var missing = beatmapIds.Distinct().Where(id => !cache.TryGetValue(id, out _) && !loads.ContainsKey(id)).ToArray();
        if (missing.Length < 2)
            return loaded;

        try
        {
            var compressed = await store.LoadCompressedManyAsync(missing, operation, cancellationToken);
            await Parallel.ForEachAsync(compressed, cancellationToken, async (entry, token) =>
            {
                var load = loads.GetOrAdd(
                    entry.Key,
                    id => new Lazy<Task<MemoryWorkingBeatmap>>(
                        async () => Insert(id, await store.DecompressAsync(entry.Value, operation, CancellationToken.None), operation),
                        LazyThreadSafetyMode.ExecutionAndPublication));
                try
                {
                    loaded[entry.Key] = await load.Value.WaitAsync(token);
                }
                finally
                {
                    if (load.IsValueCreated && load.Value.IsCompleted)
                        loads.TryRemove(new KeyValuePair<int, Lazy<Task<MemoryWorkingBeatmap>>>(entry.Key, load));
                }
            });
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            logger.LogWarning(exception, "Batch beatmap prefetch failed; falling back to per-beatmap loads");
        }

        return loaded;
    }

    private async Task<MemoryWorkingBeatmap> LoadAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        return Insert(beatmapId, await store.GetRawBeatmapAsync(beatmapId, operation, cancellationToken), operation);
    }

    private MemoryWorkingBeatmap Insert(int beatmapId, byte[] raw, string operation)
    {
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
