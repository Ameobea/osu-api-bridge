using DiffCalc.Configuration;
using Microsoft.Extensions.Options;

namespace DiffCalc.Api;

public sealed class RequestValidator(IOptions<DiffCalcOptions> options)
{
    private readonly DiffCalcOptions options = options.Value;

    public IReadOnlyList<string> Validate(CalculationBatchRequest? batch)
    {
        var errors = new List<string>();
        if (batch?.Calculations is null)
        {
            errors.Add("calculations is required");
            return errors;
        }

        if (batch.Operation is not null && !CalculationOperations.IsKnown(batch.Operation))
            errors.Add("operation must be one of hiscores, simulate_single, or simulate_batch");

        if (batch.Calculations.Count == 0)
            errors.Add("calculations must not be empty");
        if (batch.Calculations.Count > options.MaxBatchSize)
            errors.Add($"calculations exceeds the maximum batch size of {options.MaxBatchSize}");

        for (var i = 0; i < Math.Min(batch.Calculations.Count, options.MaxBatchSize); i++)
        {
            var request = batch.Calculations[i];
            var prefix = $"calculations[{i}]";
            if (request.BeatmapId <= 0)
                errors.Add($"{prefix}.beatmap_id must be positive");
            if (request.RequestId?.Length > 128)
                errors.Add($"{prefix}.request_id exceeds 128 characters");
            if (request.Mods?.Count > 16)
                errors.Add($"{prefix}.mods exceeds 16 entries");

            var acronyms = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var mod in request.Mods ?? [])
            {
                var acronym = mod.Acronym?.Trim();
                if (string.IsNullOrWhiteSpace(acronym) || acronym.Length is < 2 or > 3 || !acronym.All(char.IsAsciiLetterOrDigit))
                    errors.Add($"{prefix}.mods contains an invalid acronym");
                else if (!acronyms.Add(acronym))
                    errors.Add($"{prefix}.mods contains duplicate acronym {acronym}");

                if (mod.Settings?.Count > 16)
                    errors.Add($"{prefix}.mods[{acronym}].settings exceeds 16 entries");
                foreach (var (key, value) in mod.Settings ?? new Dictionary<string, System.Text.Json.JsonElement>())
                {
                    if (key.Length is < 1 or > 64)
                        errors.Add($"{prefix}.mods[{acronym}].settings contains an invalid key");
                    if (value.ValueKind is System.Text.Json.JsonValueKind.Array or System.Text.Json.JsonValueKind.Object or System.Text.Json.JsonValueKind.Undefined)
                        errors.Add($"{prefix}.mods[{acronym}].settings.{key} must be a primitive value");
                }
            }

            if (!request.IsClassic && acronyms.Contains("CL"))
                errors.Add($"{prefix} cannot specify CL when is_classic is false");

            if (request.Score is { } score)
            {
                if (!double.IsFinite(score.Accuracy) || score.Accuracy is < 0 or > 100)
                    errors.Add($"{prefix}.score.accuracy must be finite and in [0, 100]");
                if (score.MaxCombo < 0)
                    errors.Add($"{prefix}.score.max_combo must not be negative");
                if (score.LegacyTotalScore < 0)
                    errors.Add($"{prefix}.score.legacy_total_score must not be negative");

                if (score.Statistics is { } statistics)
                {
                    var values = new int?[]
                    {
                        statistics.Great, statistics.Ok, statistics.Meh, statistics.Miss,
                        statistics.LargeTickHit, statistics.LargeTickMiss, statistics.SmallTickHit,
                        statistics.SmallTickMiss, statistics.SliderTailHit, statistics.LargeBonus,
                        statistics.SmallBonus
                    };
                    if (values.Any(value => value < 0))
                        errors.Add($"{prefix}.score.statistics values must not be negative");

                    var suppliedMainResults = new[] { statistics.Great, statistics.Ok, statistics.Meh };
                    if (suppliedMainResults.Any(value => value.HasValue) && suppliedMainResults.Any(value => !value.HasValue))
                        errors.Add($"{prefix}.score.statistics great, ok, and meh must be supplied together");
                }
            }
        }

        return errors;
    }
}
