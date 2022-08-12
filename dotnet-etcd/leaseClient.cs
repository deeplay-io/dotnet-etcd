// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using dotnet_etcd.multiplexer;
using Etcdserverpb;
using Polly;

using Grpc.Core;
using Polly.Contrib.WaitAndRetry;
using Polly.Timeout;

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
        /// </summary>
        /// <param name="leaseId"></param>
        /// <param name="cancellationToken"></param>
        public async Task LeaseKeepAlive(long leaseId, CancellationToken cancellationToken) => await CallEtcdAsync(async (connection) =>
                                                                                             {
                                                                                                 using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                                                                                     connection._leaseClient.LeaseKeepAlive(cancellationToken: cancellationToken))
                                                                                                 {
                                                                                                     LeaseKeepAliveRequest request = new LeaseKeepAliveRequest
                                                                                                     {
                                                                                                         ID = leaseId
                                                                                                     };

                                                                                                     while (true)
                                                                                                     {
                                                                                                         cancellationToken.ThrowIfCancellationRequested();

                                                                                                         await leaser.RequestStream.WriteAsync(request).ConfigureAwait(false);
                                                                                                         if (!await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                                                                                         {
                                                                                                             await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                                             throw new EndOfStreamException();
                                                                                                         }

                                                                                                         LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                                                                                         if (update.ID != leaseId || update.TTL == 0) // expired
                                                                                                         {
                                                                                                             await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                                             return;
                                                                                                         }

                                                                                                         await Task.Delay(TimeSpan.FromMilliseconds(update.TTL * 1000 / 3), cancellationToken).ConfigureAwait(false);
                                                                                                     }
                                                                                                 }
                                                                                             }).ConfigureAwait(false);


        /// <summary>
        /// HighlyReliableLeaseKeepAlive keeps lease alive by sending keep alive requests and receiving keep alive responses.
        /// Reliability is achieved by sequentially sending keep alive requests at short intervals to all etcd nodes
        /// </summary>
        /// <param name="leaseId">lease identifier</param>
        /// <param name="leaseRemainigTTL">the remaining TTL at the time the method was called. used to determine initial deadlines</param>
        /// <param name="tryDurationMs"></param>
        /// <param name="maxRetryBackoffMs"></param>
        /// <param name="sleepAfterSuccessMs"></param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="LeaseExpiredOrNotFoundException">throws an exception if no response
        /// is received within the lease TTL or <paramref name="leaseRemainigTTL"></paramref> </exception>
        public async Task HighlyReliableLeaseKeepAliveAsync(long leaseId, long leaseRemainigTTL,
            int tryDurationMs, int maxRetryBackoffMs, int sleepAfterSuccessMs, CancellationToken cancellationToken)
        {
            int startNodeIndex = (new Random()).Next(_balancer._numNodes);
            while (true) // keepAlive  rounds
            {
                cancellationToken.ThrowIfCancellationRequested();
                var roundCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                int usedKeepAliveJobs = 0;
                int delayBetweenUseNewKeepAliveJob = tryDurationMs / _balancer._numNodes;
                startNodeIndex = ++startNodeIndex >= _balancer._numNodes ? 0 : startNodeIndex;
                DateTime leaseExpiredAt = DateTime.Now.ToUniversalTime().AddSeconds(leaseRemainigTTL);
                List<Task<LeaseKeepAliveResponse>> keepAliveJobs = new List<Task<LeaseKeepAliveResponse>>();
                while (usedKeepAliveJobs == 0) //< _balancer._numNodes)
                {
                    usedKeepAliveJobs++;
                    roundCancellationTokenSource.Token.ThrowIfCancellationRequested();
                    int currentNodeIndex = startNodeIndex + usedKeepAliveJobs;
                    currentNodeIndex = currentNodeIndex >= _balancer._numNodes
                        ? currentNodeIndex - _balancer._numNodes
                        : currentNodeIndex;
                    Connection connection = _balancer._healthyNode.ElementAt(currentNodeIndex);

                    Task<LeaseKeepAliveResponse> keepAliveJob = await InvokeWithWaitLimitAsync(
                        () => OneTimeKeepAliveWithRetryAsync(
                            leaseId,
                            connection,
                            tryDurationMs,
                            maxRetryBackoffMs,
                            leaseExpiredAt,
                            roundCancellationTokenSource.Token),
                        waitLimitMs: delayBetweenUseNewKeepAliveJob,
                        cancellationToken: roundCancellationTokenSource.Token);
                    keepAliveJobs.Add(keepAliveJob);
                    if (keepAliveJob.IsCompletedSuccessfully)
                    {
                        roundCancellationTokenSource.Cancel();
                        break;
                    }
                }

                await Task.WhenAll(keepAliveJobs);
                if (IsAnyTaskCompletedSuccessfully(
                        keepAliveJobs,
                        out var keepAliveResponse)
                    && keepAliveResponse.TTL > 0)
                {
                    //lease not found, expired or revoked
                    await Task.Delay(
                        sleepAfterSuccessMs,
                        cancellationToken);
                    leaseRemainigTTL = Math.Max(0,keepAliveResponse.TTL - sleepAfterSuccessMs / 1000);
                    continue; //go to next round
                }

                List<Exception> exceptions = new List<Exception>()
                {
                    new LeaseExpiredOrNotFoundException(leaseId),
                };
                exceptions.AddRange(
                    keepAliveJobs
                        .Where(job => !job.IsCompletedSuccessfully)
                        .Select(job => job.Exception)); // collect all exceptions
                throw new AggregateException(exceptions);
            }

            async Task<Task<T>> InvokeWithWaitLimitAsync<T>(Func<Task<T>> func, int waitLimitMs, CancellationToken cancellationToken)
            {
                Task waitLimit = Task.Delay(
                    waitLimitMs,
                    cancellationToken);
                var responseTask = func();
                await Task.WhenAny(responseTask,waitLimit).ConfigureAwait(false);
                return responseTask;
            }



            async Task<LeaseKeepAliveResponse> OneTimeKeepAliveWithRetryAsync(long leaseId, Connection connection,
                int tryDurationMs, int maxRetryBackoffMs, DateTime deadline, CancellationToken cancellationToken)
            {
                Console.WriteLine("OneTimeKeepAliveWithRetryAsync");
                cancellationToken.ThrowIfCancellationRequested();
                // timeoutPolicy thrown own exception, that overlap retry exceptions,
                // so this list used for catch the retry exceptions.
                List<Exception> retryExceptions = new List<Exception>();
                var timeout = deadline.ToUniversalTime() - DateTime.Now.ToUniversalTime();
                var timeoutPolicy = Policy.TimeoutAsync(timeout);
                TimeSpan maxRetryBackoff = TimeSpan.FromMilliseconds(maxRetryBackoffMs);
                var delay = Backoff.DecorrelatedJitterBackoffV2(
                        fastFirst: true,
                        medianFirstRetryDelay: TimeSpan.FromMilliseconds(100),
                        retryCount: int.MaxValue)
                    .Select(
                        s => TimeSpan.FromTicks(
                            Math.Min(
                                s.Ticks,
                                maxRetryBackoff.Ticks)));
                var retryPolicy = Policy
                    .Handle<Exception>(e => e is LeaseExpiredOrNotFoundException == false)
                    .WaitAndRetryAsync(
                        delay,
                        onRetry: (exception, _) => retryExceptions.Add(exception));
                var retryTimeoutPolicy = Policy.TimeoutAsync(TimeSpan.FromMilliseconds(tryDurationMs));
                var policy =
                    Policy.WrapAsync(
                        timeoutPolicy,
                        retryPolicy,
                        retryTimeoutPolicy);
                try
                {
                    var response = await policy.ExecuteAsync(
                        continueOnCapturedContext: false,
                        cancellationToken: cancellationToken,
                        action: async retryCancellationToken =>
                        {
                            retryCancellationToken.ThrowIfCancellationRequested();
                            using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                   connection._leaseClient
                                       .LeaseKeepAlive(cancellationToken: retryCancellationToken))
                            {
                                await leaser.RequestStream.WriteAsync(
                                    new LeaseKeepAliveRequest() { ID = leaseId },
                                    cancellationToken).ConfigureAwait(false);
                                bool result = await leaser.ResponseStream.MoveNext(retryCancellationToken)
                                    .ConfigureAwait(false);
                                if (!result)
                                {
                                    throw new RpcException(
                                        new Status(
                                            StatusCode.Aborted,
                                            "didnt receive keepAlive response"));
                                }

                                await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                return leaser.ResponseStream.Current;
                            }
                        });
                    return response;
                }
                catch (TimeoutRejectedException e)
                {
                    throw new AggregateException(
                        retryExceptions
                            .Append(e)
                            .Reverse());
                }
            }

            bool IsAnyTaskCompletedSuccessfully<T>(IEnumerable<Task<T>> tasks,
                out T response)
            {
                foreach (Task<T> call in tasks)
                {
                    if (call.IsCompletedSuccessfully)
                    {
                        response = call.Result;
                        return true;
                    }
                }

                response = default;
                return false;
            }

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
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await CallEtcdAsync(async (connection) =>
                                                                                     {
                                                                                         using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                                                                             connection._leaseClient
                                                                                                 .LeaseKeepAlive(headers, cancellationToken: cancellationToken))
                                                                                         {
                                                                                             Task leaserTask = Task.Run(async () =>
                                                                                             {
                                                                                                 while (await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                                                                                 {
                                                                                                     LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                                                                                     method(update);
                                                                                                 }
                                                                                             }, cancellationToken);

                                                                                             await leaser.RequestStream.WriteAsync(request).ConfigureAwait(false);
                                                                                             await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                             await leaserTask.ConfigureAwait(false);
                                                                                         }
                                                                                     }).ConfigureAwait(false);

        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="request">The request to send to the server.</param>
        /// <param name="methods"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest request, Action<LeaseKeepAliveResponse>[] methods,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await CallEtcdAsync(async (connection) =>
                                                                                     {
                                                                                         using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                                                                             connection._leaseClient
                                                                                                 .LeaseKeepAlive(headers, cancellationToken: cancellationToken))
                                                                                         {
                                                                                             Task leaserTask = Task.Run(async () =>
                                                                                             {
                                                                                                 while (await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                                                                                 {
                                                                                                     LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                                                                                     foreach (Action<LeaseKeepAliveResponse> method in methods)
                                                                                                     {
                                                                                                         method(update);
                                                                                                     }

                                                                                                 }
                                                                                             }, cancellationToken);

                                                                                             await leaser.RequestStream.WriteAsync(request).ConfigureAwait(false);
                                                                                             await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                             await leaserTask.ConfigureAwait(false);
                                                                                         }
                                                                                     }).ConfigureAwait(false);


        /// <summary>
        /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        /// to the server and streaming keep alive responses from the server to the client.
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="method"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        public async Task LeaseKeepAlive(LeaseKeepAliveRequest[] requests, Action<LeaseKeepAliveResponse> method,
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null) => await CallEtcdAsync(async (connection) =>
                                                                                     {
                                                                                         using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                                                                             connection._leaseClient
                                                                                                 .LeaseKeepAlive(headers, cancellationToken: cancellationToken))
                                                                                         {
                                                                                             Task leaserTask = Task.Run(async () =>
                                                                                             {
                                                                                                 while (await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                                                                                 {
                                                                                                     LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                                                                                     method(update);
                                                                                                 }
                                                                                             }, cancellationToken);

                                                                                             foreach (LeaseKeepAliveRequest request in requests)
                                                                                             {
                                                                                                 await leaser.RequestStream.WriteAsync(request).ConfigureAwait(false);
                                                                                             }

                                                                                             await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                             await leaserTask.ConfigureAwait(false);
                                                                                         }
                                                                                     }).ConfigureAwait(false);

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
            CancellationToken cancellationToken, Grpc.Core.Metadata headers = null, DateTime? deadline = null) => await CallEtcdAsync(async (connection) =>
                                                                                                                {
                                                                                                                    using (AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> leaser =
                                                                                                                        connection._leaseClient
                                                                                                                            .LeaseKeepAlive(headers, deadline, cancellationToken))
                                                                                                                    {
                                                                                                                        Task leaserTask = Task.Run(async () =>
                                                                                                                        {
                                                                                                                            while (await leaser.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                                                                                                                            {
                                                                                                                                LeaseKeepAliveResponse update = leaser.ResponseStream.Current;
                                                                                                                                foreach (Action<LeaseKeepAliveResponse> method in methods)
                                                                                                                                {
                                                                                                                                    method(update);
                                                                                                                                }

                                                                                                                            }
                                                                                                                        }, cancellationToken);

                                                                                                                        foreach (LeaseKeepAliveRequest request in requests)
                                                                                                                        {
                                                                                                                            await leaser.RequestStream.WriteAsync(request).ConfigureAwait(false);
                                                                                                                        }

                                                                                                                        await leaser.RequestStream.CompleteAsync().ConfigureAwait(false);
                                                                                                                        await leaserTask.ConfigureAwait(false);
                                                                                                                    }
                                                                                                                }).ConfigureAwait(false);

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
