using System.Net;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Etcdserverpb;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace DevelopmentSandbox // Note: actual namespace depends on the project name.
{
    internal class Program
    {
        static void Main(string[] args)
        {
            ILogger logger = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Console(
                    theme: SystemConsoleTheme.Literate,
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}{Properties:j}{NewLine}")
                .Enrich.FromLogContext()
                .CreateLogger();

            HttpMessageHandler handler = new SocketsHttpHandler
            {

                // ConnectTimeout = default,
                KeepAlivePingDelay = TimeSpan.FromMilliseconds(1000),
                KeepAlivePingTimeout = TimeSpan.FromMilliseconds(1000),
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
                //
                // PooledConnectionIdleTimeout = default,
                // PooledConnectionLifetime = default,
                // ResponseDrainTimeout = default
            };
            EtcdClient client  = new EtcdClient(
                "http://127.0.0.1:2379", //todo: вытащить в конфигурацию
                handler: handler,
                useLegacyRpcExceptionForCancellation: false,
                interceptors: new GrpcLogsInterceptor(
                    logger,
                    new LogsInterceptorOptions
                    {
                        //LoggerName = null,
                        IncludeLogData = true
                    }));
        

            Task.Run(
                async () =>
                {
                    while (true)
                    {
                        await Task.Delay(500);
                        client.Put(
                            "1",
                            DateTime.Now.ToString());
                    }
                
                });

            void Rsp(WatchResponse response)
            {
                logger.ForContext(
                        "watchEvent",
                        response)
                    .Information("new watch event");
            }

            client.WatchRange(
                "",
                method: (Action<WatchResponse>)Rsp);
        
        
            logger.Information("endddd");
        }
    }
};