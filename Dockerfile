FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/IngestSvc/IngestSvc.csproj src/IngestSvc/
RUN dotnet restore src/IngestSvc/IngestSvc.csproj
COPY src/ src/
RUN dotnet publish src/IngestSvc/IngestSvc.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER app
ENTRYPOINT ["dotnet", "IngestSvc.dll"]
