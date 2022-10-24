using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Integration;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace DevelopmentSandbox; // Note: actual namespace depends on the project name.



internal class Program
{
    private static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        AppDomain.CurrentDomain.ProcessExit += (_, _) => { cts.Cancel(); };
        Console.CancelKeyPress += (_, ea) => { cts.Cancel(); };
        ILogger logger = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Console(
                theme: SystemConsoleTheme.Literate,
                outputTemplate:
                "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}{Properties:j}{NewLine}")
            .Enrich.FromLogContext()
            .CreateLogger();

        var connection_string = Environment.GetEnvironmentVariable("ETCD_CONNECTION_STRING");

        
        var client = new EtcdClient(
            connectionString: connection_string,
            // handler: handler,
            useLegacyRpcExceptionForCancellation: false);


        var txn = new TxnRequest();
        for (int i = 0; i < 120; i++)
        {
            txn.Success.Add(new RequestOp()
            {
                RequestPut = new PutRequest()
                {
                    Key = ByteString.CopyFromUtf8( "asdfadsfasdfdsf"+i),
                    Value = ByteString.CopyFromUtf8("dfasdvasdfasdf")
                    
                }
            });
        }

        while (true)
        {
            try
            {
                await client.TransactionAsync(
                    txn,
                    deadline: DateTime.UtcNow.AddMilliseconds(10));

                Console.WriteLine("ok");
            }
            catch (Exception e)
            {
                Console.WriteLine("fail");
            }
        }

       
        Func<Task> doJob = async () =>
        {
            var leaseId = client.LeaseGrant(new LeaseGrantRequest() { TTL = 5 }).ID;
            await client.HighlyReliableLeaseKeepAliveAsync(
                leaseId,
                5,
                retryDurationMs: 1000,
                maxRetryBackoffMs: 400,
                sleepAfterSuccessMs: 5000 / 3,
                cts.Token).ConfigureAwait(false);
            // await client.LeaseKeepAlive(
            //     leaseId,
            //     CancellationToken.None).ConfigureAwait(false);
        };
        
        List<Task> jobs = new List<Task>(1000);
        
        foreach (var i in Enumerable.Range(
                     0,
                     20000))
        {
        
            await Task.Delay(5); //что бы кипалайвы были в приоритете создания новых тасков
            if (cts.Token.IsCancellationRequested)
            {
                break;
            }
        
            var t = Task.Run(
                async () =>
                {
                    cts.Token.ThrowIfCancellationRequested();
                    Console.WriteLine(i);
                    try
                    {
                        await doJob().ConfigureAwait(false);
                    }
                    finally
                    {
                        cts.Cancel();
                    }
                },
                cts.Token);
            jobs.Add(t);
        }
        
        await await Task.WhenAny(jobs);
    }
}