using DiffCalc.Api;
using DiffCalc.Beatmaps;
using DiffCalc.Configuration;
using Microsoft.Extensions.Options;

namespace DiffCalc.Calculation;

public sealed class BatchCalculationService(
    OsuCalculationEngine engine,
    IOptions<DiffCalcOptions> options,
    ILogger<BatchCalculationService> logger)
{
    private readonly DiffCalcOptions options = options.Value;

    public async Task<CalculationBatchResponse> CalculateAsync(
        IReadOnlyList<CalculationRequest> calculations,
        string operation,
        CancellationToken cancellationToken)
    {
        var results = new CalculationResult[calculations.Count];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, calculations.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = options.EffectiveCalculationConcurrency,
                CancellationToken = cancellationToken
            },
            async (index, token) =>
            {
                var request = calculations[index];
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeout.CancelAfter(TimeSpan.FromSeconds(options.CalculationTimeoutSeconds));
                try
                {
                    results[index] = await engine.CalculateAsync(request, operation, timeout.Token);
                }
                catch (BeatmapNotFoundException exception)
                {
                    results[index] = Error(request, "beatmap_not_found", exception.Message);
                }
                catch (ArgumentException exception)
                {
                    results[index] = Error(request, "invalid_calculation", exception.Message);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    results[index] = Error(request, "calculation_timeout", "The calculation timed out.");
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "Calculation failed for beatmap {BeatmapId}", request.BeatmapId);
                    results[index] = Error(request, "calculation_failed", "The calculation failed.");
                }
            });

        return new CalculationBatchResponse
        {
            Algorithm = new AlgorithmInfo
            {
                OsuGamePackageVersion = OsuCalculationEngine.OsuGamePackageVersion,
                DifficultyVersion = engine.DifficultyVersion
            },
            Results = results
        };
    }

    private static CalculationResult Error(CalculationRequest request, string code, string message) => new()
    {
        RequestId = request.RequestId,
        BeatmapId = request.BeatmapId,
        Error = new CalculationError { Code = code, Message = message }
    };
}
