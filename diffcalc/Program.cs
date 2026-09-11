using System.Text.Json;
using System.Text.Json.Serialization;
using DiffCalc.Api;
using DiffCalc.Beatmaps;
using DiffCalc.Calculation;
using DiffCalc.Configuration;
using DiffCalc.Health;
using DiffCalc.Observability;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using osu.Game.Beatmaps.Formats;
using Prometheus;

LegacyDifficultyCalculatorBeatmapDecoder.Register();
osu.Framework.Logging.Logger.Enabled = false;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true).AddEnvironmentVariables();

builder.Services
    .AddOptions<DiffCalcOptions>()
    .Bind(builder.Configuration.GetSection(DiffCalcOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options => options.ApiPort != options.MetricsPort, "API and metrics ports must differ.")
    .ValidateOnStart();

var startupOptions = builder.Configuration
    .GetSection(DiffCalcOptions.SectionName)
    .Get<DiffCalcOptions>() ?? new DiffCalcOptions();

builder.WebHost.ConfigureKestrel(server =>
{
    server.Limits.MaxRequestBodySize = startupOptions.MaxRequestBodyBytes;
    server.Limits.RequestHeadersTimeout = TimeSpan.FromSeconds(10);
    server.ListenLocalhost(startupOptions.ApiPort);
    server.ListenLocalhost(startupOptions.MetricsPort);
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
    options.SerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
});

builder.Services.AddHttpClient("beatmap-download", client =>
{
    client.Timeout = TimeSpan.FromSeconds(30);
    client.DefaultRequestHeaders.UserAgent.ParseAdd("osu-api-bridge-diffcalc/1.0");
}).UseHttpClientMetrics();
builder.Services.AddSingleton<BeatmapStore>();
builder.Services.AddSingleton<CachedBeatmapProvider>();
builder.Services.AddSingleton<OsuCalculationEngine>();
builder.Services.AddSingleton<BatchCalculationService>();
builder.Services.AddSingleton<RequestValidator>();
builder.Services.AddSingleton<MySqlHealthCheck>();
builder.Services.AddHealthChecks()
    .AddCheck<MySqlHealthCheck>("mysql", tags: ["ready"])
    .ForwardToPrometheus();

var app = builder.Build();
var options = app.Services.GetRequiredService<IOptions<DiffCalcOptions>>().Value;

app.UseExceptionHandler(handler => handler.Run(async context =>
{
    var exception = context.Features.Get<IExceptionHandlerFeature>()?.Error;
    context.RequestServices.GetRequiredService<ILoggerFactory>()
        .CreateLogger("UnhandledException")
        .LogError(exception, "Unhandled diffcalc request exception");
    if (context.Request.Path.StartsWithSegments("/v1/calculate"))
        DiffCalcMetrics.Requests.WithLabels("calculate", "error").Inc();
    context.Response.StatusCode = StatusCodes.Status500InternalServerError;
    await context.Response.WriteAsJsonAsync(new { error = "internal_error" });
}));
app.UseHttpMetrics(httpOptions => httpOptions.ReduceStatusCodeCardinality());
app.UseMiddleware<ApiKeyMiddleware>();

app.MapPost("/v1/calculate", async (
    CalculationBatchRequest request,
    RequestValidator validator,
    BatchCalculationService calculations,
    CancellationToken cancellationToken) =>
{
    var errors = validator.Validate(request);
    if (errors.Count > 0)
    {
        DiffCalcMetrics.Requests.WithLabels("calculate", "invalid").Inc();
        return Results.BadRequest(new ValidationProblemResponse
        {
            Error = "invalid_request",
            Details = errors
        });
    }

    var operation = CalculationOperations.Normalize(request.Operation);
    DiffCalcMetrics.BatchSize.WithLabels(operation).Observe(request.Calculations!.Count);
    using var timer = DiffCalcMetrics.BatchDuration.WithLabels(operation).NewTimer();
    var response = await calculations.CalculateAsync(request.Calculations, operation, cancellationToken);
    DiffCalcMetrics.Requests.WithLabels("calculate", "success").Inc();
    return Results.Ok(response);
}).RequireHost($"*:{options.ApiPort}");

app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false })
    .RequireHost($"*:{options.ApiPort}");
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") })
    .RequireHost($"*:{options.ApiPort}");
app.MapMetrics("/metrics").RequireHost($"*:{options.MetricsPort}");

await app.RunAsync();
