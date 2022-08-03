using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Etcdserverpb;
using NUnit.Framework;

namespace Integration;

public class DevelopmentProcessTests
{
    private EtcdClient Client { get; } = new EtcdClient(
        "http://127.0.0.1:2379", //todo: вытащить в конфигурацию
        useLegacyRpcExceptionForCancellation: false,
        interceptors: new GrpcLogsInterceptor(
            Framework.Logger,
            new LogsInterceptorOptions
            {
                //LoggerName = null,
                IncludeLogData = true
            }));

    [SetUp]
    public async Task Setup()
    {
        await Framework.CleanEtcdTestsKeys(Client);
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        await Framework.CleanEtcdTestsKeys(Client);
    }

    //debug space here
    [Test]
    public async Task Test1()
    {
        Trace.Listeners.Add(new ConsoleTraceListener());
        // var rsp = await Client.LeaseGrantAsync(new LeaseGrantRequest() { TTL = 8 });
        // var ct = new CancellationTokenSource();
        // await Client.LeaseKeepAlive(
        //     rsp.ID,
        //     30,
        //     ct.Token);

        Task.Run(
            () =>
            {
                while (true)
                {
                    Task.Delay(500).GetAwaiter().GetResult();
                    Client.Put(
                                        "1",
                                        DateTime.Now.ToString());
                }
                
            });

        void Rsp(WatchResponse response)
        {
            Framework.Logger.ForContext(
                    "watchEvent",
                    response)
                .Information("new watch event");
        }

        Client.WatchRange(
            "",
            method: (Action<WatchResponse>)Rsp);
    }
}
