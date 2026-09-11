using System.Globalization;
using System.Text.Json;
using DiffCalc.Api;
using osu.Game.Online.API;
using osu.Game.Rulesets.Mods;
using osu.Game.Rulesets.Osu;

namespace DiffCalc.Calculation;

public static class ModParser
{
    public static Mod[] Parse(CalculationRequest request, OsuRuleset ruleset)
    {
        var inputs = (request.Mods ?? []).ToList();
        if (request.IsClassic && inputs.All(mod => !mod.Acronym.Equals("CL", StringComparison.OrdinalIgnoreCase)))
            inputs.Add(new ModInput { Acronym = "CL" });

        var mods = new List<Mod>(inputs.Count);
        foreach (var input in inputs)
        {
            var acronym = input.Acronym.Trim().ToUpperInvariant();
            if (ruleset.CreateModFromAcronym(acronym) is null)
                throw new ArgumentException($"Unsupported osu! mod acronym: {acronym}");

            var apiMod = new APIMod { Acronym = acronym };
            foreach (var (key, value) in input.Settings ?? new Dictionary<string, JsonElement>())
                apiMod.Settings[key] = ConvertSetting(value);
            mods.Add(apiMod.ToMod(ruleset));
        }

        return mods.ToArray();
    }

    public static string BuildCacheKey(int beatmapId, IReadOnlyList<Mod> mods)
    {
        var normalizedMods = mods
            .Select(mod => new APIMod(mod))
            .OrderBy(mod => mod.Acronym, StringComparer.Ordinal)
            .Select(mod => $"{mod.Acronym}:{string.Join(',', mod.Settings.OrderBy(x => x.Key).Select(x => $"{x.Key}={Convert.ToString(x.Value, CultureInfo.InvariantCulture)}"))}");
        return $"{beatmapId}|{string.Join('|', normalizedMods)}";
    }

    private static object ConvertSetting(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString() ?? string.Empty,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Number when value.TryGetInt64(out var integer) => integer,
        JsonValueKind.Number when value.TryGetDouble(out var number) && double.IsFinite(number) => number,
        JsonValueKind.Null => throw new ArgumentException("Mod setting values cannot be null."),
        _ => throw new ArgumentException("Mod setting values must be finite primitive values.")
    };
}
