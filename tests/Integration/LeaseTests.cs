using System;
using System.Data.Common;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Etcdserverpb;
using Grpc.Core;
using NUnit.Framework;
using Polly;

namespace Integration;

public class LeaseTests
{
    private const string ConnectionString = "http://127.0.0.1:23790,http://127.0.0.1:23790,http://127.0.0.1:23790";

    [SetUp]
    public async Task Setup()
    {
    }

    [TearDown]
    public async Task TearDownAsync()
    {
    }
    
    [Test]
    public async Task LeaseGranded()
    {
        var delegateInterceptor = new DelegateInterceptor<LeaseGrantRequest, LeaseGrantResponse>();
        var client = new EtcdClient(
            connectionString: ConnectionString,
            interceptors: delegateInterceptor);
        LeaseGrantRequest request = new() { ID = 777, TTL = 777 };
        LeaseGrantResponse response = new() { ID = 888, TTL = 888 };
        var responseTask = Task.Run(() =>
        {
            return client.LeaseGrantAsync(request);
        });
        await foreach (LeaseGrantRequest req in delegateInterceptor.ReadAllRequests(CancellationToken.None))
        {
            Assert.AreEqual(req, request);
            await delegateInterceptor.WriteResponseAsync(response);
            break;
        }

        var rsp = responseTask.Result;
        Assert.AreEqual(rsp, response);
    }
    
    [Test]
    public async Task AfterExceptionsLeaseGranded()
    {
        var delegateInterceptor = new DelegateInterceptor<LeaseGrantRequest, LeaseGrantResponse>();
        var client = new EtcdClient(
            connectionString: ConnectionString,
            interceptors: delegateInterceptor);
        LeaseGrantRequest request = new() { ID = 777, TTL = 777 };
        LeaseGrantResponse response = new() { ID = 888, TTL = 888 };
        var responseTask = Task.Run(() =>
        {
            return client.LeaseGrantAsync(request);
        });
        RpcException unavailableException = new RpcException(
            new Status(
                StatusCode.Unavailable,
                ""));

        var iterator = delegateInterceptor.ReadAllRequests(CancellationToken.None).GetAsyncEnumerator();
        await iterator.MoveNextAsync();
        var req = iterator.Current;
        await delegateInterceptor.WriteResponseAsync(unavailableException);
        await delegateInterceptor.WriteResponseAsync(unavailableException);
        await delegateInterceptor.WriteResponseAsync(response);
        var rsp = responseTask.Result;
        
        Assert.AreEqual(rsp, response);
    }

    [Test]
    public async Task AfterThreeExceptionsLeaseGrandedFail()
    {
        var delegateInterceptor = new DelegateInterceptor<LeaseGrantRequest, LeaseGrantResponse>();
        var client = new EtcdClient(
            connectionString: ConnectionString,
            interceptors:
            delegateInterceptor);
        LeaseGrantRequest request = new() { ID = 777, TTL = 777 };
        var responseTask = Task.Run(() => { return client.LeaseGrantAsync(request); });
        RpcException unavailableException = new RpcException(
            new Status(
                StatusCode.Unavailable,
                ""));
        await delegateInterceptor.WriteResponseAsync(unavailableException);
        await delegateInterceptor.WriteResponseAsync(unavailableException);
        await delegateInterceptor.WriteResponseAsync(unavailableException);
        var ex = Assert.Throws<RpcException>(
            () =>
            {
                try
                {
                    responseTask.Wait();
                }
                catch (AggregateException e)
                {

                    throw e.InnerException;
                }
            });
        Assert.That(ex.Status.StatusCode, Is.EqualTo(StatusCode.Unavailable));
    }

    [Test]
    public async Task LeaseKeepAliveRequestSendedAfterDelay()
    {
        var delegateInterceptor = new DelegateInterceptor<LeaseKeepAliveRequest, LeaseKeepAliveResponse>();
        var client = new EtcdClient(
            connectionString: ConnectionString,
            interceptors:
            delegateInterceptor);
        var responseTask = Task.Run(
            () =>
            {
                return client.LeaseKeepAlive(
                    777,
                    CancellationToken.None);
            });
        var iterator = delegateInterceptor.ReadAllRequests(CancellationToken.None).GetAsyncEnumerator();
        await iterator.MoveNextAsync();
        var req = iterator.Current;
        Assert.AreEqual(req.ID, 777);
        await delegateInterceptor.WriteResponseAsync(new LeaseKeepAliveResponse() { ID = 777, TTL = 1 });
        var nextKeepAliveTask = iterator.MoveNextAsync();
        var ex = Assert.Throws<TimeoutException>(
            () => nextKeepAliveTask.AsTask()
                .WaitAsync(TimeSpan.FromMilliseconds(100))
                .GetAwaiter().GetResult());
        await Task.Delay(300);
        Assert.True(nextKeepAliveTask.Result);
    }
}
