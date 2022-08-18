using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Integration;

public class DelegateInterceptor<TReq, TRsp> : Interceptor
    where TReq : class
    where TRsp : class
{
    private readonly MessageChannel<TReq> _requests = new();

    private readonly MessageChannel<TRsp> _responses = new();

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();

        Task.Run(() => _requests.CastMessageType<TRequest>().WriteAsync(request)).Wait();
        var responseReceived = Task.Run(() => _responses.MoveNext()).Result;
        if (!responseReceived) throw new Exception("response required");
        var response = _responses.CastMessageType<TResponse>().Current;
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

        Task.Run(() => _requests.WriteAsync((request as TReq)!)).Wait();
        var responseReceived = Task.Run(() => _responses.MoveNext()).Result;
        if (!responseReceived) throw new Exception("response required");
        return _requests.CastMessageType<TResponse>().Current;
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();

        var call = new AsyncClientStreamingCall<TRequest, TResponse>(
            requestStream: _requests.CastMessageType<TRequest>(),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { },
            responseAsync: _responses.MoveNext().ContinueWith(moveNextask =>
            {
                var responseReceived = moveNextask.Result;
                if (!responseReceived) throw new Exception("response required");
                return _responses.CastMessageType<TResponse>().Current;
            }));
        return call;
    }


    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        ValidateCall<TRequest, TResponse>();
        
        Task.Run(() => _requests.WriteAsync((request as TReq)!)).Wait();
        var call = new AsyncServerStreamingCall<TResponse>(
            responseStream: _responses.CastMessageType<TResponse>(),
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
        AsyncDuplexStreamingCall<TRequest, TResponse> call = new(
            requestStream: _requests.CastMessageType<TRequest>(),
            responseStream: _responses.CastMessageType<TResponse>(),
            responseHeadersAsync: Task.FromResult(new Metadata()),
            getStatusFunc: () => new Status(
                statusCode: StatusCode.OK,
                detail: ""),
            getTrailersFunc: () => new Metadata(),
            disposeAction: () => { });
        return call;
    }


    public async Task WriteResponseAsync(TRsp rsp)
    {
        await _responses.WriteAsync(rsp);
    }

    public async Task WriteResponseAsync(Exception exception)
    {
        await _responses.WriteExceptionAsync(exception);
    }

    public async Task CloseResponseStreamAsync()
    {
        await _responses.CompleteAsync();
    }

    public IAsyncEnumerable<TReq> ReadAllRequests(CancellationToken cancellationToken)
    {
        return _requests.ReadAllAsync(cancellationToken);
    }

    private static void ValidateCall<TRequest, TResponse>()
    {
        if (typeof(TReq) != typeof(TRequest) || typeof(TRsp) != typeof(TResponse))
            throw new Exception("Interceptor not applicable to these call");
    }
}