using System.Buffers;
using System.Diagnostics;
using System.IO.Compression;
using DiffCalc.Configuration;
using DiffCalc.Observability;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Prometheus;

namespace DiffCalc.Beatmaps;

public sealed class BeatmapNotFoundException(int beatmapId)
    : Exception($"Beatmap {beatmapId} was not found.");

public sealed class BeatmapStore
{
    private readonly string connectionString;
    private readonly HttpClient httpClient;
    private readonly DiffCalcOptions options;
    private readonly ILogger<BeatmapStore> logger;
    private readonly SemaphoreSlim downloadGate = new(1, 1);
    private DateTimeOffset lastDownloadStarted = DateTimeOffset.MinValue;

    public BeatmapStore(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        IOptions<DiffCalcOptions> options,
        ILogger<BeatmapStore> logger)
    {
        connectionString = configuration.GetConnectionString("OsuTrack")
            ?? throw new InvalidOperationException("ConnectionStrings:OsuTrack must be configured.");
        httpClient = httpClientFactory.CreateClient("beatmap-download");
        this.options = options.Value;
        this.logger = logger;
    }

    public async Task<byte[]> GetRawBeatmapAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        var compressed = await LoadCompressedAsync(beatmapId, operation, cancellationToken);
        if (compressed is null)
            compressed = await DownloadAndStoreAsync(beatmapId, operation, cancellationToken);

        using var timer = DiffCalcMetrics.BeatmapDecompressionDuration.WithLabels(operation).NewTimer();
        await using var input = new MemoryStream(compressed, writable: false);
        await using var gzip = new GZipStream(input, CompressionMode.Decompress);
        return await ReadBoundedAsync(gzip, options.MaxDecompressedBeatmapBytes, cancellationToken);
    }

    public async Task<bool> CanConnectAsync(CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new MySqlCommand("SELECT 1", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
    }

    private async Task<byte[]?> LoadCompressedAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        using var timer = DiffCalcMetrics.BeatmapDatabaseDuration.WithLabels(operation, "load").NewTimer();
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new MySqlCommand(
                "SELECT raw_beatmap_gzipped FROM fetched_beatmaps WHERE beatmap_id = @beatmap_id",
                connection);
            command.Parameters.AddWithValue("@beatmap_id", beatmapId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            DiffCalcMetrics.BeatmapDatabaseOperations.WithLabels(operation, "load", result is null ? "miss" : "hit").Inc();
            return result as byte[];
        }
        catch
        {
            DiffCalcMetrics.BeatmapDatabaseOperations.WithLabels(operation, "load", "error").Inc();
            throw;
        }
    }

    private async Task<byte[]> DownloadAndStoreAsync(
        int beatmapId,
        string operation,
        CancellationToken cancellationToken)
    {
        await downloadGate.WaitAsync(cancellationToken);
        try
        {
            var existing = await LoadCompressedAsync(beatmapId, operation, cancellationToken);
            if (existing is not null)
                return existing;

            var nextAllowed = lastDownloadStarted.AddMilliseconds(options.BeatmapDownloadIntervalMs);
            var delay = nextAllowed - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, cancellationToken);
            lastDownloadStarted = DateTimeOffset.UtcNow;

            var stopwatch = Stopwatch.StartNew();
            try
            {
                using var response = await httpClient.GetAsync(
                    $"{options.BeatmapDownloadBaseUrl}{beatmapId}",
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);
                if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                    throw new BeatmapNotFoundException(beatmapId);
                response.EnsureSuccessStatusCode();

                if (response.Content.Headers.ContentLength > options.MaxDecompressedBeatmapBytes)
                    throw new InvalidDataException("Downloaded beatmap exceeds the configured size limit.");

                await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                var raw = await ReadBoundedAsync(responseStream, options.MaxDecompressedBeatmapBytes, cancellationToken);
                var compressed = Compress(raw);
                if (compressed.Length > options.MaxCompressedBeatmapBytes)
                    throw new InvalidDataException("Compressed beatmap exceeds the configured size limit.");

                await StoreCompressedAsync(beatmapId, compressed, operation, cancellationToken);
                DiffCalcMetrics.BeatmapDownloads.WithLabels(operation, "success").Inc();
                logger.LogInformation("Downloaded and cached missing beatmap {BeatmapId}", beatmapId);
                return compressed;
            }
            catch (BeatmapNotFoundException)
            {
                DiffCalcMetrics.BeatmapDownloads.WithLabels(operation, "not_found").Inc();
                throw;
            }
            catch
            {
                DiffCalcMetrics.BeatmapDownloads.WithLabels(operation, "error").Inc();
                throw;
            }
            finally
            {
                DiffCalcMetrics.BeatmapDownloadDuration.WithLabels(operation).Observe(stopwatch.Elapsed.TotalSeconds);
            }
        }
        finally
        {
            downloadGate.Release();
        }
    }

    private async Task StoreCompressedAsync(
        int beatmapId,
        byte[] compressed,
        string operation,
        CancellationToken cancellationToken)
    {
        using var timer = DiffCalcMetrics.BeatmapDatabaseDuration.WithLabels(operation, "store").NewTimer();
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new MySqlCommand(
                "INSERT IGNORE INTO fetched_beatmaps (beatmap_id, raw_beatmap_gzipped) VALUES (@beatmap_id, @raw)",
                connection);
            command.Parameters.AddWithValue("@beatmap_id", beatmapId);
            command.Parameters.AddWithValue("@raw", compressed);
            await command.ExecuteNonQueryAsync(cancellationToken);
            DiffCalcMetrics.BeatmapDatabaseOperations.WithLabels(operation, "store", "success").Inc();
        }
        catch
        {
            DiffCalcMetrics.BeatmapDatabaseOperations.WithLabels(operation, "store", "error").Inc();
            throw;
        }
    }

    private static byte[] Compress(byte[] raw)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
            gzip.Write(raw);
        return output.ToArray();
    }

    private static async Task<byte[]> ReadBoundedAsync(Stream stream, int maxBytes, CancellationToken cancellationToken)
    {
        using var output = new MemoryStream(Math.Min(maxBytes, 128 * 1024));
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (true)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(), cancellationToken);
                if (read == 0)
                    return output.ToArray();
                if (output.Length + read > maxBytes)
                    throw new InvalidDataException("Beatmap exceeds the configured size limit.");
                output.Write(buffer, 0, read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
