FROM mcr.microsoft.com/dotnet/sdk:6.0.302 AS build
WORKDIR /source

COPY . .
WORKDIR /source/perf/CloudEventKafkaTester
RUN dotnet publish -c release -o /app

# final stage/image
FROM mcr.microsoft.com/dotnet/aspnet:6.0.3
ENV TZ="Asia/Shanghai"
WORKDIR /app
COPY --from=build /app ./
ENTRYPOINT ["dotnet", "CloudEventKafkaTester.dll"]
