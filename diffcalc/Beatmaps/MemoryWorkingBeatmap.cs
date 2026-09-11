using osu.Framework.Audio.Track;
using osu.Framework.Graphics.Textures;
using osu.Game.Beatmaps;
using osu.Game.Beatmaps.Formats;
using osu.Game.IO;
using osu.Game.Rulesets.Osu;
using osu.Game.Skinning;

namespace DiffCalc.Beatmaps;

/// <summary>A reusable decoded beatmap. GetPlayableBeatmap clones the source before applying mods.</summary>
public sealed class MemoryWorkingBeatmap : WorkingBeatmap
{
    private readonly Beatmap beatmap;

    public MemoryWorkingBeatmap(ReadOnlyMemory<byte> rawBeatmap, int beatmapId)
        : this(Decode(rawBeatmap), beatmapId)
    {
    }

    private MemoryWorkingBeatmap(Beatmap beatmap, int beatmapId)
        : base(beatmap.BeatmapInfo, null)
    {
        this.beatmap = beatmap;
        beatmap.BeatmapInfo.OnlineID = beatmapId;
        beatmap.BeatmapInfo.Ruleset = new OsuRuleset().RulesetInfo;
    }

    protected override IBeatmap GetBeatmap() => beatmap;

    public override Texture GetBackground() => throw new NotSupportedException();
    protected override Track GetBeatmapTrack() => throw new NotSupportedException();
    protected override ISkin GetSkin() => throw new NotSupportedException();
    public override Stream GetStream(string storagePath) => throw new NotSupportedException();

    private static Beatmap Decode(ReadOnlyMemory<byte> rawBeatmap)
    {
        using var stream = new MemoryStream(rawBeatmap.ToArray(), writable: false);
        using var reader = new LineBufferedReader(stream);
        return Decoder.GetDecoder<Beatmap>(reader).Decode(reader);
    }
}
