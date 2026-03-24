FROM mcr.microsoft.com/dotnet/sdk:8.0 AS builder
WORKDIR /app
COPY *.csproj .
RUN dotnet restore
COPY *.cs .
RUN dotnet publish -c Release -o /out

FROM mcr.microsoft.com/dotnet/runtime:8.0
COPY --from=builder /out/ /app/
ENTRYPOINT ["dotnet", "/app/FerrosJepsen.dll"]
