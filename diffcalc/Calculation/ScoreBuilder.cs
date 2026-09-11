using DiffCalc.Api;
using osu.Game.Beatmaps;
using osu.Game.Rulesets.Mods;
using osu.Game.Rulesets.Osu.Mods;
using osu.Game.Rulesets.Scoring;
using osu.Game.Scoring;

namespace DiffCalc.Calculation;

public static class ScoreBuilder
{
    public static ScoreInfo Build(
        ScoreInput input,
        BeatmapInfo beatmapInfo,
        int totalObjects,
        int maxCombo,
        int sliderCount,
        Mod[] mods,
        osu.Game.Rulesets.RulesetInfo rulesetInfo)
    {
        var statistics = BuildStatistics(input, totalObjects, sliderCount, mods);
        var score = new ScoreInfo(beatmapInfo, rulesetInfo)
        {
            Accuracy = input.Accuracy / 100,
            MaxCombo = input.MaxCombo ?? maxCombo,
            Statistics = statistics,
            LegacyTotalScore = input.LegacyTotalScore,
            Mods = mods
        };
        return score;
    }

    private static Dictionary<HitResult, int> BuildStatistics(
        ScoreInput input,
        int totalObjects,
        int sliderCount,
        IReadOnlyList<Mod> mods)
    {
        var source = input.Statistics;
        int great;
        int ok;
        int meh;
        var miss = source?.Miss ?? 0;

        if (source?.Great is not null && source.Ok is not null && source.Meh is not null)
        {
            great = source.Great.Value;
            ok = source.Ok.Value;
            meh = source.Meh.Value;
            if (great + ok + meh + miss != totalObjects)
                throw new ArgumentException("great + ok + meh + miss must equal the beatmap object count.");
        }
        else
        {
            (great, ok, meh, miss) = GenerateMainResults(totalObjects, input.Accuracy / 100, miss);
        }

        var result = new Dictionary<HitResult, int>
        {
            [HitResult.Great] = great,
            [HitResult.Ok] = ok,
            [HitResult.Meh] = meh,
            [HitResult.Miss] = miss
        };

        Add(result, HitResult.LargeTickHit, source?.LargeTickHit);
        Add(result, HitResult.LargeTickMiss, source?.LargeTickMiss);
        Add(result, HitResult.SmallTickHit, source?.SmallTickHit);
        Add(result, HitResult.SmallTickMiss, source?.SmallTickMiss);
        Add(result, HitResult.SliderTailHit, source?.SliderTailHit);
        Add(result, HitResult.LargeBonus, source?.LargeBonus);
        Add(result, HitResult.SmallBonus, source?.SmallBonus);

        // Match osu-tools simulation semantics. Non-classic scores participate in
        // slider accuracy, and omitted slider statistics mean a perfect slider run.
        var usesClassicSliderAccuracy = mods
            .OfType<OsuModClassic>()
            .Any(mod => mod.NoSliderHeadAccuracy.Value);
        if (!usesClassicSliderAccuracy)
        {
            result.TryAdd(HitResult.LargeTickMiss, 0);
            result.TryAdd(HitResult.SliderTailHit, sliderCount);
        }
        return result;
    }

    private static void Add(Dictionary<HitResult, int> destination, HitResult hitResult, int? value)
    {
        if (value.HasValue)
            destination[hitResult] = value.Value;
    }

    /// <summary>Matches the official osu-tools simulation strategy for an accuracy-only score.</summary>
    internal static (int Great, int Ok, int Meh, int Miss) GenerateMainResults(
        int totalObjects,
        double accuracy,
        int miss)
    {
        if (miss > totalObjects)
            throw new ArgumentException("miss exceeds the beatmap object count.");

        var relevantCount = totalObjects - miss;
        if (relevantCount == 0)
            return (0, 0, 0, totalObjects);

        var relevantAccuracy = Math.Clamp(accuracy * totalObjects / relevantCount, 0, 1);
        int ok;
        int meh;

        if (relevantAccuracy >= 0.25)
        {
            var ratioMehToOk = Math.Pow(1 - (relevantAccuracy - 0.25) / 0.75, 2);
            var okEstimate = 6 * relevantCount * (1 - relevantAccuracy) / (5 * ratioMehToOk + 4);
            var mehEstimate = okEstimate * ratioMehToOk;
            ok = (int)Math.Round(okEstimate);
            meh = (int)Math.Round(okEstimate + mehEstimate) - ok;
        }
        else if (relevantAccuracy >= 1.0 / 6)
        {
            var okEstimate = 6 * relevantCount * relevantAccuracy - relevantCount;
            ok = (int)Math.Round(okEstimate);
            meh = relevantCount - ok;
        }
        else
        {
            ok = 0;
            meh = (int)Math.Round(6 * relevantCount * relevantAccuracy);
            miss = totalObjects - meh;
        }

        var great = totalObjects - ok - meh - miss;
        return (great, ok, meh, miss);
    }
}
