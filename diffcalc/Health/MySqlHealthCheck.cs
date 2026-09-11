using DiffCalc.Beatmaps;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DiffCalc.Health;

public sealed class MySqlHealthCheck(BeatmapStore store) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await store.CanConnectAsync(cancellationToken)
                ? HealthCheckResult.Healthy()
                : HealthCheckResult.Unhealthy("MySQL connectivity check returned false.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("MySQL connectivity check failed.", exception);
        }
    }
}
