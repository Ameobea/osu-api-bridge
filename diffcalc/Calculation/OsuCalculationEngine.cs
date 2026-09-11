using System.Collections.Concurrent;
using System.Diagnostics;
using DiffCalc.Api;
using DiffCalc.Beatmaps;
using DiffCalc.Configuration;
using DiffCalc.Observability;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using osu.Game.Rulesets.Difficulty;
using osu.Game.Rulesets.Mods;
using osu.Game.Rulesets.Osu;
using osu.Game.Rulesets.Osu.Difficulty;
using osu.Game.Utils;
using Prometheus;

namespace DiffCalc.Calculation;

public sealed class OsuCalculationEngine
{
    public const string OsuGamePackageVersion = "2026.730.0";

    private readonly CachedBeatmapProvider beatmaps;
    private readonly MemoryCache difficultyCache;
    private readonly ConcurrentDictionary<string, Lazy<Task<CachedDifficulty>>> difficultyLoads = new();
    private readonly SemaphoreSlim calculationGate;
    private readonly TimeSpan calculationTimeout;
    private readonly ILogger<OsuCalculationEngine> logger;
    private int difficultyCacheEntries;
    private int lastDifficultyVersion;

    public OsuCalculationEngine(
        CachedBeatmapProvider beatmaps,
        IOptions<DiffCalcOptions> options,
        ILogger<OsuCalculationEngine> logger)
    {
        this.beatmaps = beatmaps;
        difficultyCache = new MemoryCache(new MemoryCacheOptions { SizeLimit = options.Value.DifficultyCacheSize });
        calculationGate = new SemaphoreSlim(options.Value.EffectiveCalculationConcurrency);
        calculationTimeout = TimeSpan.FromSeconds(options.Value.CalculationTimeoutSeconds);
        this.logger = logger;
    }

    public int DifficultyVersion => Volatile.Read(ref lastDifficultyVersion);

    public async Task<CalculationResult> CalculateAsync(
        CalculationRequest request,
        string operation,
        CancellationToken cancellationToken)
    {
        var working = await beatmaps.GetAsync(request.BeatmapId, operation, cancellationToken);
        var ruleset = new OsuRuleset();
        var mods = ModParser.Parse(request, ruleset);
        var cacheKey = ModParser.BuildCacheKey(request.BeatmapId, mods);
        var cachedDifficulty = await GetDifficultyAsync(cacheKey, working, ruleset, mods, operation, cancellationToken);
        Volatile.Write(ref lastDifficultyVersion, cachedDifficulty.Version);

        if (request.Score is null)
        {
            return new CalculationResult
            {
                RequestId = request.RequestId,
                BeatmapId = request.BeatmapId,
                Difficulty = cachedDifficulty.Result
            };
        }

        var queueStopwatch = Stopwatch.StartNew();
        await calculationGate.WaitAsync(cancellationToken);
        DiffCalcMetrics.CalculationQueueDuration.WithLabels(operation).Observe(queueStopwatch.Elapsed.TotalSeconds);
        DiffCalcMetrics.CalculationsInProgress.WithLabels(operation).Inc();
        try
        {
            using var timer = DiffCalcMetrics.CalculationDuration.WithLabels(operation, "performance").NewTimer();
            try
            {
                var score = ScoreBuilder.Build(
                    request.Score,
                    working.BeatmapInfo,
                    cachedDifficulty.ObjectCount,
                    cachedDifficulty.Attributes.MaxCombo,
                    cachedDifficulty.Attributes.SliderCount,
                    mods,
                    ruleset.RulesetInfo);
                var calculator = ruleset.CreatePerformanceCalculator()
                    ?? throw new InvalidOperationException("osu! performance calculator is unavailable.");
                var attributes = calculator.Calculate(score, cachedDifficulty.Attributes);
                var performance = ToResult((OsuPerformanceAttributes)attributes);
                DiffCalcMetrics.Calculations.WithLabels(operation, "performance", "success").Inc();

                return new CalculationResult
                {
                    RequestId = request.RequestId,
                    BeatmapId = request.BeatmapId,
                    Difficulty = cachedDifficulty.Result,
                    Performance = performance
                };
            }
            catch
            {
                DiffCalcMetrics.Calculations.WithLabels(operation, "performance", "error").Inc();
                throw;
            }
        }
        finally
        {
            DiffCalcMetrics.CalculationsInProgress.WithLabels(operation).Dec();
            calculationGate.Release();
        }
    }

    private async Task<CachedDifficulty> GetDifficultyAsync(
        string key,
        MemoryWorkingBeatmap working,
        OsuRuleset ruleset,
        Mod[] mods,
        string operation,
        CancellationToken cancellationToken)
    {
        if (difficultyCache.TryGetValue(key, out CachedDifficulty? cached) && cached is not null)
        {
            DiffCalcMetrics.DifficultyCacheAccess.WithLabels(operation, "hit").Inc();
            return cached;
        }

        DiffCalcMetrics.DifficultyCacheAccess.WithLabels(operation, "miss").Inc();
        var load = difficultyLoads.GetOrAdd(
            key,
            _ => new Lazy<Task<CachedDifficulty>>(
                () => ComputeDifficultyAsync(key, working, ruleset, mods, operation),
                LazyThreadSafetyMode.ExecutionAndPublication));
        try
        {
            return await load.Value.WaitAsync(cancellationToken);
        }
        finally
        {
            if (load.IsValueCreated && load.Value.IsCompleted)
                difficultyLoads.TryRemove(new KeyValuePair<string, Lazy<Task<CachedDifficulty>>>(key, load));
        }
    }

    private async Task<CachedDifficulty> ComputeDifficultyAsync(
        string key,
        MemoryWorkingBeatmap working,
        OsuRuleset ruleset,
        Mod[] mods,
        string operation)
    {
        var queueStopwatch = Stopwatch.StartNew();
        await calculationGate.WaitAsync();
        DiffCalcMetrics.CalculationQueueDuration.WithLabels(operation).Observe(queueStopwatch.Elapsed.TotalSeconds);
        DiffCalcMetrics.CalculationsInProgress.WithLabels(operation).Inc();
        try
        {
            using var timer = DiffCalcMetrics.CalculationDuration.WithLabels(operation, "difficulty").NewTimer();
            try
            {
                using var timeout = new CancellationTokenSource(calculationTimeout);
                var calculator = ruleset.CreateDifficultyCalculator(working);
                var attributes = (OsuDifficultyAttributes)calculator.Calculate(mods, timeout.Token);
                var playable = working.GetPlayableBeatmap(ruleset.RulesetInfo, mods, timeout.Token);
                var result = ToResult(attributes, playable.Difficulty, ModUtils.CalculateRateWithMods(mods));
                var cached = new CachedDifficulty(attributes, calculator.Version, result, playable.HitObjects.Count);
                difficultyCache.Set(
                    key,
                    cached,
                    new MemoryCacheEntryOptions()
                        .SetSize(1)
                        .RegisterPostEvictionCallback((_, _, _, state) => ((OsuCalculationEngine)state!).OnDifficultyEvicted(), this));
                Interlocked.Increment(ref difficultyCacheEntries);
                DiffCalcMetrics.DifficultyCacheEntries.Set(Volatile.Read(ref difficultyCacheEntries));
                DiffCalcMetrics.Calculations.WithLabels(operation, "difficulty", "success").Inc();
                return cached;
            }
            catch
            {
                DiffCalcMetrics.Calculations.WithLabels(operation, "difficulty", "error").Inc();
                throw;
            }
        }
        finally
        {
            DiffCalcMetrics.CalculationsInProgress.WithLabels(operation).Dec();
            calculationGate.Release();
        }
    }

    private void OnDifficultyEvicted()
    {
        Interlocked.Decrement(ref difficultyCacheEntries);
        DiffCalcMetrics.DifficultyCacheEntries.Set(Volatile.Read(ref difficultyCacheEntries));
    }

    private static DifficultyResult ToResult(
        OsuDifficultyAttributes attributes,
        osu.Game.Beatmaps.BeatmapDifficulty beatmapDifficulty,
        double clockRate) => new()
    {
        Stars = attributes.StarRating,
        Aim = attributes.AimDifficulty,
        Speed = attributes.SpeedDifficulty,
        Flashlight = attributes.FlashlightDifficulty,
        Reading = attributes.ReadingDifficulty,
        SliderFactor = attributes.SliderFactor,
        SpeedNoteCount = attributes.SpeedNoteCount,
        MaxCombo = attributes.MaxCombo,
        HitCircleCount = attributes.HitCircleCount,
        SliderCount = attributes.SliderCount,
        SpinnerCount = attributes.SpinnerCount,
        ApproachRate = beatmapDifficulty.ApproachRate,
        OverallDifficulty = beatmapDifficulty.OverallDifficulty,
        CircleSize = beatmapDifficulty.CircleSize,
        DrainRate = beatmapDifficulty.DrainRate,
        ClockRate = clockRate
    };

    private static PerformanceResult ToResult(OsuPerformanceAttributes attributes) => new()
    {
        Pp = attributes.Total,
        Aim = attributes.Aim,
        Speed = attributes.Speed,
        Accuracy = attributes.Accuracy,
        Flashlight = attributes.Flashlight,
        Reading = attributes.Reading,
        EffectiveMissCount = attributes.EffectiveMissCount,
        SpeedDeviation = attributes.SpeedDeviation
    };

    private sealed record CachedDifficulty(
        OsuDifficultyAttributes Attributes,
        int Version,
        DifficultyResult Result,
        int ObjectCount);
}
