using System.ComponentModel.DataAnnotations;

namespace DiffCalc.Configuration;

public sealed class DiffCalcOptions
{
    public const string SectionName = "DiffCalc";

    [Range(1, 65535)]
    public int ApiPort { get; init; } = 4512;

    [Range(1, 65535)]
    public int MetricsPort { get; init; } = 4513;

    [Range(1, 4096)]
    public int MaxBatchSize { get; init; } = 256;

    [Range(0, 1024)]
    public int MaxConcurrentCalculations { get; init; }

    [Range(1, 300)]
    public int CalculationTimeoutSeconds { get; init; } = 15;

    [Range(1024, 16 * 1024 * 1024)]
    public long MaxRequestBodyBytes { get; init; } = 256 * 1024;

    [Range(16 * 1024 * 1024, long.MaxValue)]
    public long BeatmapCacheSizeBytes { get; init; } = 1024L * 1024 * 1024;

    [Range(1, 1_000_000)]
    public int DifficultyCacheSize { get; init; } = 50_000;

    [Range(1024, 128 * 1024 * 1024)]
    public int MaxCompressedBeatmapBytes { get; init; } = 8 * 1024 * 1024;

    [Range(1024, 256 * 1024 * 1024)]
    public int MaxDecompressedBeatmapBytes { get; init; } = 32 * 1024 * 1024;

    [Range(0, 60_000)]
    public int BeatmapDownloadIntervalMs { get; init; } = 1200;

    [Required, Url]
    public string BeatmapDownloadBaseUrl { get; init; } = "https://osu.ppy.sh/osu/";

    [Required, MinLength(32)]
    public string ApiKey { get; init; } = string.Empty;

    public int EffectiveCalculationConcurrency =>
        MaxConcurrentCalculations > 0 ? MaxConcurrentCalculations : Math.Max(1, Environment.ProcessorCount);
}
