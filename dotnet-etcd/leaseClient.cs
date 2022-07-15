// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Etcdserverpb;

using Grpc.Core;

namespace dotnet_etcd
{
    public partial class EtcdClient
    {
        /// <summary>
        /// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
        /// within a given time to live period. All keys attached to the lease will be expired and
        /// deleted if the lease expires. Each expired key generates a delete event in the event history.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The response received from the server.</returns>
        public LeaseGrantResponse LeaseGrant(LeaseGrantRequest request, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default) => CallEtcd((connection) => connection._leaseClient
                                                                            .LeaseGrant(request, headers, deadline, cancellationToken));

        /// <summary>
        /// LeaseGrant creates a lease in async which expires if the server does not receive a keepAlive
        /// within a given time to live period. All keys attached to the lease will be expired and
        /// deleted if the lease expires. Each expired key generates a delete event in the event history.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The response received from the server.</returns>
        public async Task<LeaseGrantResponse> LeaseGrantAsync(LeaseGrantRequest request,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default) => await CallEtcdAsync(async (connection) => await connection._leaseClient
                                                                            .LeaseGrantAsync(request, headers, deadline, cancellationToken)).ConfigureAwait(false);

        /// <summary>
        /// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The response received from the server.</returns>
        public LeaseRevokeResponse LeaseRevoke(LeaseRevokeRequest request, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default) => CallEtcd((connection) => connection._leaseClient
                                                                            .LeaseRevoke(request, headers, deadline, cancellationToken));

        /// <summary>
        /// LeaseRevoke revokes a lease in async. All keys attached to the lease will expire and be deleted.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The response received from the server.</returns>
        public async Task<LeaseRevokeResponse> LeaseRevokeAsync(LeaseRevokeRequest request,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default) => await CallEtcdAsync(async (connection) => await connection._leaseClient
                                                                            .LeaseRevokeAsync(request, headers, deadline, cancellationToken)).ConfigureAwait(false);

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// Task will be complited if lease expired or revoked
        /// </summary>
        /// <param name="leaseId"></param>
        /// <param name="cancellationToken"></param>
        public async Task LeaseKeepAlive(long leaseId, CancellationToken cancellationToken)
        {
            var leaseExpiredToken = new TaskCompletionSource<int>();
            var keepAliveTask = LeaseKeepAlive(
                request: new LeaseKeepAliveRequest { ID = leaseId },
                method: response =>
                {
                    if (response.ID != leaseId || response.TTL == 0) // expired or not found
                    {
                        leaseExpiredToken.SetResult(0);
                    }
                },
                cancellationToken);
            await Task.WhenAny(
                leaseExpiredToken.Task,
                keepAliveTask).Unwrap();
        }

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="method"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest request, Action<LeaseKeepAliveResponse> method,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await LeaseKeepAlive(
            new[] { request },
            new[] { method },
            cancellationToken,
            headers);

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="methods"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest request, Action<LeaseKeepAliveResponse>[] methods,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await LeaseKeepAlive(
            new[] { request },
            methods,
            cancellationToken,
            headers);

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="method"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest[] requests, Action<LeaseKeepAliveResponse> method,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await LeaseKeepAlive(
            requests,
            new[] { method },
            cancellationToken,
            headers);

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="methods"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest[] requests, Action<LeaseKeepAliveResponse>[] methods,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null, DateTime? deadline = null)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    var connection = _balancer.GetConnection();
                    using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                           connection._leaseClient
                               .LeaseKeepAlive(
                                   headers,
                                   deadline,
                                   cancellationToken))
                    {
                        Channel<LeaseKeepAliveResponse> leasesCh = Channel.CreateUnbounded<LeaseKeepAliveResponse>();
                        Task leaserTask = Task.Run(
                            async () =>
                            {
                                while (await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                {
                                    retryCount = 0;
                                    LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                    foreach (Action<LeaseKeepAliveResponse> method in methods)
                                    {
                                        method(update);
                                    }

                                    await leasesCh.Writer.WriteAsync(
                                        update,
                                        cancellationToken);
                                }
                            },
                            cancellationToken);

                        foreach (LeaseKeepAliveRequest request in requests)
                        {
                            await leaser.RequestStream
                                .WriteAsync(request)
                                .ConfigureAwait(false);
                        }

                        List<Task<LeaseKeepAliveResponse>> leases = new List<Task<LeaseKeepAliveResponse>>();
                        while (cancellationToken.IsCancellationRequested == false &&
                               (leaserTask.IsCompleted == false || leases.Count > 0))
                        {
                            await Task.WhenAny(
                                leases.Select(t => t as Task)
                                    .Append(leasesCh.Reader.WaitToReadAsync(cancellationToken).AsTask()));
                            if (leasesCh.Reader.TryRead(out var leaseKeepAliveResponse)
                                && leaseKeepAliveResponse.TTL > 0)
                            {
                                Console.WriteLine("keepAlive" + DateTime.Now);
                                leases.Add(
                                    Task.Delay(
                                        TimeSpan.FromMilliseconds(100),//leaseKeepAliveResponse.TTL / 3.0 * 1000),
                                        cancellationToken).ContinueWith(
                                        t => leaseKeepAliveResponse,
                                        cancellationToken));
                            }

                            foreach (Task<LeaseKeepAliveResponse> keepAliveResponseTask in leases
                                         .Where(t => t.IsCompleted).ToArray())
                            {
                                var keepAliveResponse = keepAliveResponseTask.Result;
                                await leaser.RequestStream
                                    .WriteAsync(new LeaseKeepAliveRequest() { ID = keepAliveResponse.ID })
                                    .ConfigureAwait(false);
                                leases.Remove(keepAliveResponseTask);
                            }
                        }

                        await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                        await leaserTask.ConfigureAwait(false);
                    }
                }
                catch (Exception)
                {
                    retryCount++;
                    if (retryCount >= _balancer._numNodes)
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// LeaseTimeToLive retrieves lease information.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The response received from the server.</returns>
        public LeaseTimeToLiveResponse LeaseTimeToLive(LeaseTimeToLiveRequest request,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default) => CallEtcd((connection) => connection._leaseClient
                                                                            .LeaseTimeToLive(request, headers, deadline, cancellationToken));

        /// <summary>
        /// LeaseTimeToLive retrieves lease information.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>The call object.</returns>
        public async Task<LeaseTimeToLiveResponse> LeaseTimeToLiveAsync(LeaseTimeToLiveRequest request,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default) => await CallEtcdAsync(async (connection) => await connection._leaseClient
                                                                            .LeaseTimeToLiveAsync(request, headers, deadline, cancellationToken)).ConfigureAwait(false);
    }
}
