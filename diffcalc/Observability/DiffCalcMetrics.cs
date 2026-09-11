using Prometheus;

namespace DiffCalc.Observability;

public static class DiffCalcMetrics
{
    public static readonly Counter Requests = Metrics.CreateCounter(
        "diffcalc_requests_total",
        "Internal diffcalc API requests.",
        new CounterConfiguration { LabelNames = ["endpoint", "status"] });

    public static readonly Histogram BatchSize = Metrics.CreateHistogram(
        "diffcalc_batch_size",
        "Number of calculations in a batch.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [1, 4, 16, 32, 64, 96, 128, 192, 256]
        });

    public static readonly Histogram BatchDuration = Metrics.CreateHistogram(
        "diffcalc_batch_duration_seconds",
        "Time spent processing an accepted calculation batch.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.4, 0.6, 0.8, 1, 1.25, 1.5, 1.75, 2, 2.5, 5]
        });

    public static readonly Counter Calculations = Metrics.CreateCounter(
        "diffcalc_calculations_total",
        "Difficulty and performance calculations.",
        new CounterConfiguration { LabelNames = ["operation", "kind", "status"] });

    public static readonly Histogram CalculationDuration = Metrics.CreateHistogram(
        "diffcalc_calculation_duration_seconds",
        "Canonical osu! calculation duration.",
        new HistogramConfiguration
        {
            LabelNames = ["operation", "kind"],
            Buckets = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]
        });

    public static readonly Histogram CalculationQueueDuration = Metrics.CreateHistogram(
        "diffcalc_calculation_queue_duration_seconds",
        "Time spent waiting for a calculation worker.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]
        });

    public static readonly Gauge CalculationsInProgress = Metrics.CreateGauge(
        "diffcalc_calculations_in_progress",
        "Canonical osu! calculations currently running.",
        new GaugeConfiguration { LabelNames = ["operation"] });

    public static readonly Counter BeatmapCacheAccess = Metrics.CreateCounter(
        "diffcalc_beatmap_cache_access_total",
        "Parsed beatmap cache accesses.",
        new CounterConfiguration { LabelNames = ["operation", "result"] });

    public static readonly Gauge BeatmapCacheEntries = Metrics.CreateGauge(
        "diffcalc_beatmap_cache_entries",
        "Parsed beatmaps currently cached.");

    public static readonly Gauge BeatmapCacheEstimatedBytes = Metrics.CreateGauge(
        "diffcalc_beatmap_cache_estimated_bytes",
        "Estimated bytes used by parsed beatmaps.");

    public static readonly Counter DifficultyCacheAccess = Metrics.CreateCounter(
        "diffcalc_difficulty_cache_access_total",
        "Difficulty attribute cache accesses.",
        new CounterConfiguration { LabelNames = ["operation", "result"] });

    public static readonly Gauge DifficultyCacheEntries = Metrics.CreateGauge(
        "diffcalc_difficulty_cache_entries",
        "Difficulty attribute entries currently cached.");

    public static readonly Histogram BeatmapDatabaseDuration = Metrics.CreateHistogram(
        "diffcalc_beatmap_database_duration_seconds",
        "Time spent loading or storing beatmaps in MySQL.",
        new HistogramConfiguration
        {
            LabelNames = ["operation", "action"],
            Buckets = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]
        });

    public static readonly Counter BeatmapDatabaseOperations = Metrics.CreateCounter(
        "diffcalc_beatmap_database_operations_total",
        "MySQL beatmap operations.",
        new CounterConfiguration { LabelNames = ["operation", "action", "status"] });

    public static readonly Histogram BeatmapDecompressionDuration = Metrics.CreateHistogram(
        "diffcalc_beatmap_decompression_duration_seconds",
        "Time spent decompressing cached .osu files.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1]
        });

    public static readonly Histogram BeatmapParseDuration = Metrics.CreateHistogram(
        "diffcalc_beatmap_parse_duration_seconds",
        "Time spent parsing .osu files with the official decoder.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2.5]
        });

    public static readonly Counter BeatmapDownloads = Metrics.CreateCounter(
        "diffcalc_beatmap_downloads_total",
        "Beatmap downloads from osu!.",
        new CounterConfiguration { LabelNames = ["operation", "status"] });

    public static readonly Histogram BeatmapDownloadDuration = Metrics.CreateHistogram(
        "diffcalc_beatmap_download_duration_seconds",
        "Beatmap download duration.",
        new HistogramConfiguration
        {
            LabelNames = ["operation"],
            Buckets = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30]
        });
}
