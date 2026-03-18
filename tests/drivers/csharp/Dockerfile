FROM mcr.microsoft.com/dotnet/sdk:8.0 AS builder
WORKDIR /tests
COPY CqlSmokeTest.csproj .
RUN dotnet restore
COPY Program.cs .
RUN dotnet publish -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=builder /app .
CMD ["dotnet", "CqlSmokeTest.dll"]
