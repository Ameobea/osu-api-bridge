FROM mcr.microsoft.com/dotnet/sdk:8.0-bookworm-slim AS build

WORKDIR /src
COPY DiffCalc.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish --configuration Release --no-restore --output /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim

WORKDIR /app
COPY --from=build /app ./

ENV DOTNET_EnableDiagnostics=0
USER app
ENTRYPOINT ["dotnet", "DiffCalc.dll"]
