using System.Threading.Tasks;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace Integration;

public static class Framework
{
    public const string TestPrefix = "/Tests/";

    public static ILogger Logger { get; } = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Console(
            theme: SystemConsoleTheme.Literate,
            outputTemplate:
            "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}{Properties:j}{NewLine}")
        .Enrich.FromLogContext()
        .CreateLogger();

    // public static EtcdClient Client { get; } = new EtcdClient(
    //     "http://localhost:23790,http://localhost:23791,http://localhost:23792", //todo: вытащить в конфигурацию
    //     useLegacyRpcExceptionForCancellation: false,
    //     interceptors: new GrpcLogsInterceptor(
    //         Logger,
    //         new LogsInterceptorOptions
    //         {
    //             //LoggerName = null,
    //             IncludeLogData = true
    //         }));

    public static async Task CleanEtcdTestsKeys(EtcdClient client)
    {
        await client.DeleteRangeAsync(TestPrefix);
    }
    
}