using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Integration;

public class DelegateInterceptor<TReq, TRsp> : Interceptor
    where TReq : class
    where TRsp : class
{
    private readonly MessageStore _requestsStore = new();
    private readonly MessageStore _responsesStore = new();

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();


        string address = GetEtcdAdders(continuation);
        _requestsStore.WriteAsync(
            address,
            request).Wait();
        var enumerator = _responsesStore.GetReader<TResponse>(address);
        if (!enumerator.MoveNext().Result) throw new Exception("response required");
        var response = enumerator.Current;
        var call = new AsyncUnaryCall<TResponse>(
            responseAsync: Task.FromResult(response),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { });
        return call;
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();
    
        string address = GetEtcdAdders(continuation);
        _requestsStore.WriteAsync(
            address,
            request).Wait();
        var enumerator = _responsesStore.GetReader<TResponse>(address);
        if (!enumerator.MoveNext().Result) throw new Exception("response required");
        return enumerator.Current;
    }
    
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();
        string address = GetEtcdAdders(continuation);
        var reader = _responsesStore.GetReader<TResponse>(address);
        var call = new AsyncClientStreamingCall<TRequest, TResponse>(
            requestStream: _requestsStore.GetWriter<TRequest>(address),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { },
            responseAsync: Task.Run(
                async () =>
                {
                    await reader.MoveNext();
                    return reader.Current;
                }));
        return call;
    }
    
    
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();
        string address = GetEtcdAdders(continuation);
        _requestsStore.WriteAsync(
            address,
            request).Wait();
        var call = new AsyncServerStreamingCall<TResponse>(
            responseStream: _responsesStore.GetReader<TResponse>(address),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { });
        return call;
    }
    
    
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();
        string address = GetEtcdAdders(continuation);
        
        AsyncDuplexStreamingCall<TRequest, TResponse> call = new(
            requestStream: _requestsStore.GetWriter<TRequest>(address),
            responseStream: _responsesStore.GetReader<TResponse>(address),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { });
        return call;
    }


    public async Task WriteResponseAsync(string address, TRsp rsp)
    {
        await _responsesStore.WriteAsync(address,rsp);
    }

    public async Task WriteResponseAsync(string address, Exception exception)
    {
        await _responsesStore.WriteAsync(
            address,
            null,
            exception);
    }

    public async Task CloseResponseStreamAsync(string address)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<TReq> ReadAllRequests(string address, CancellationToken cancellationToken)
    {
        return _requestsStore.GetReader<TReq>(address).ReadAllAsync(cancellationToken);
    }
    
    public async IAsyncEnumerable<(string address, TReq message)> ReadAllRequests(CancellationToken cancellationToken)
    {
        await foreach (var (address, message, exception) in _requestsStore.ReadMessages())
        {
            if (exception != null) throw exception;
            yield return (address, (TReq)message!);
        }
    }

    private static void ValidateCall<TRequest, TResponse>()
    {
        if (typeof(TReq) != typeof(TRequest) || typeof(TRsp) != typeof(TResponse))
            throw new Exception("Interceptor not applicable to these call");
    }

    private static string GetEtcdAdders(Delegate continuation)
    {
        object target = continuation.Target!;
        object invoker = target.GetType().GetField("invoker",BindingFlags.Instance|BindingFlags.NonPublic)!
            .GetValue(target)!;
        GrpcChannel channel = (GrpcChannel)invoker.GetType()
            .GetProperty(
                "Channel",
                BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(invoker)!;
        return channel.Target;
    }
}