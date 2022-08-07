﻿using System.Net;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Etcdserverpb;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace DevelopmentSandbox // Note: actual namespace depends on the project name.
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            ILogger logger = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Console(
                    theme: SystemConsoleTheme.Literate,
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}{Properties:j}{NewLine}")
                .Enrich.FromLogContext()
                .CreateLogger();

            // HttpMessageHandler handler = new SocketsHttpHandler
            // {
            //
            //     // ConnectTimeout = default,
            //     KeepAlivePingDelay = TimeSpan.FromMilliseconds(1000),
            //     KeepAlivePingTimeout = TimeSpan.FromMilliseconds(1000),
            //     KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
            //     //
            //     // PooledConnectionIdleTimeout = default,
            //     // PooledConnectionLifetime = default,
            //     // ResponseDrainTimeout = default
            // };
            string connection_string = Environment.GetEnvironmentVariable("ETCD_CONNECTION_STRING");
            EtcdClient client  = new EtcdClient(connection_string,
              //  handler: handler,
                useLegacyRpcExceptionForCancellation: false//,
                // interceptors: new GrpcLogsInterceptor(
                //     logger,
                //     new LogsInterceptorOptions
                //     {
                //         //LoggerName = null,
                //         IncludeLogData = true
                //     })
              );

            Func<Task> doJob = async () =>
            {
                var leaseId = client.LeaseGrant(new LeaseGrantRequest() { TTL = 1 }).ID;
                await client.HighlyReliableLeaseKeepAlive(
                    leaseId,
                    3,
                    CancellationToken.None);
                // await client.LeaseKeepAlive(
                //     leaseId,
                //     CancellationToken.None);
            };

            var jobs = Enumerable.Range(
                0,
                100000).Select(i =>
            {
                Console.WriteLine(i);
                return doJob();
            });
            await await Task.WhenAny(jobs);
            
            logger.Information("endddd");
        }
    }
};