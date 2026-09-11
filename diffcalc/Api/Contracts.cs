using System.Text.Json;

namespace DiffCalc.Api;

public sealed record CalculationBatchRequest
{
    public string? Operation { get; init; }
    public IReadOnlyList<CalculationRequest>? Calculations { get; init; }
}

public static class CalculationOperations
{
    public const string Unknown = "unknown";
    public const string Hiscores = "hiscores";
    public const string SimulateSingle = "simulate_single";
    public const string SimulateBatch = "simulate_batch";

    public static bool IsKnown(string operation) => operation is Hiscores or SimulateSingle or SimulateBatch;

    public static string Normalize(string? operation) => operation is not null && IsKnown(operation)
        ? operation
        : Unknown;
}

public sealed record CalculationRequest
{
    public string? RequestId { get; init; }
    public int BeatmapId { get; init; }
    public IReadOnlyList<ModInput>? Mods { get; init; }
    public bool IsClassic { get; init; } = true;
    public ScoreInput? Score { get; init; }
}

public sealed record ModInput
{
    public required string Acronym { get; init; }
    public IReadOnlyDictionary<string, JsonElement>? Settings { get; init; }
}

public sealed record ScoreInput
{
    /// <summary>Accuracy as a percentage in [0, 100].</summary>
    public required double Accuracy { get; init; }
    public int? MaxCombo { get; init; }
    public long? LegacyTotalScore { get; init; }
    public ScoreStatisticsInput? Statistics { get; init; }
}

public sealed record ScoreStatisticsInput
{
    public int? Great { get; init; }
    public int? Ok { get; init; }
    public int? Meh { get; init; }
    public int Miss { get; init; }
    public int? LargeTickHit { get; init; }
    public int? LargeTickMiss { get; init; }
    public int? SmallTickHit { get; init; }
    public int? SmallTickMiss { get; init; }
    public int? SliderTailHit { get; init; }
    public int? LargeBonus { get; init; }
    public int? SmallBonus { get; init; }
}

public sealed record CalculationBatchResponse
{
    public required AlgorithmInfo Algorithm { get; init; }
    public required IReadOnlyList<CalculationResult> Results { get; init; }
}

public sealed record AlgorithmInfo
{
    public required string OsuGamePackageVersion { get; init; }
    public required int DifficultyVersion { get; init; }
}

public sealed record CalculationResult
{
    public string? RequestId { get; init; }
    public int BeatmapId { get; init; }
    public DifficultyResult? Difficulty { get; init; }
    public PerformanceResult? Performance { get; init; }
    public CalculationError? Error { get; init; }
}

public sealed record DifficultyResult
{
    public double Stars { get; init; }
    public double Aim { get; init; }
    public double Speed { get; init; }
    public double Flashlight { get; init; }
    public double Reading { get; init; }
    public double SliderFactor { get; init; }
    public double SpeedNoteCount { get; init; }
    public int MaxCombo { get; init; }
    public int HitCircleCount { get; init; }
    public int SliderCount { get; init; }
    public int SpinnerCount { get; init; }
    public double ApproachRate { get; init; }
    public double OverallDifficulty { get; init; }
    public double CircleSize { get; init; }
    public double DrainRate { get; init; }
    public double ClockRate { get; init; }
}

public sealed record PerformanceResult
{
    public double Pp { get; init; }
    public double Aim { get; init; }
    public double Speed { get; init; }
    public double Accuracy { get; init; }
    public double Flashlight { get; init; }
    public double Reading { get; init; }
    public double EffectiveMissCount { get; init; }
    public double? SpeedDeviation { get; init; }
}

public sealed record CalculationError
{
    public required string Code { get; init; }
    public required string Message { get; init; }
}

public sealed record ValidationProblemResponse
{
    public required string Error { get; init; }
    public required IReadOnlyList<string> Details { get; init; }
}
