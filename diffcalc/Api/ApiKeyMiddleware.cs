using System.Security.Cryptography;
using System.Text;
using DiffCalc.Configuration;
using DiffCalc.Observability;
using Microsoft.Extensions.Options;

namespace DiffCalc.Api;

public sealed class ApiKeyMiddleware(RequestDelegate next, IOptions<DiffCalcOptions> options)
{
    private readonly byte[] expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(options.Value.ApiKey));

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.Request.Path.StartsWithSegments("/v1"))
        {
            await next(context);
            return;
        }

        var supplied = context.Request.Headers["X-Diffcalc-Key"];
        if (supplied.Count != 1)
        {
            DiffCalcMetrics.Requests.WithLabels("calculate", "unauthorized").Inc();
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        var suppliedHash = SHA256.HashData(Encoding.UTF8.GetBytes(supplied[0]!));
        if (!CryptographicOperations.FixedTimeEquals(expectedHash, suppliedHash))
        {
            DiffCalcMetrics.Requests.WithLabels("calculate", "unauthorized").Inc();
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        await next(context);
    }
}
