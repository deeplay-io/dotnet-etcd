// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Http;
using Etcdserverpb;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Microsoft.Extensions.Logging;
using V3Lockpb;

namespace dotnet_etcd.multiplexer
{
    internal class Connection
    {
        internal KV.KVClient _kvClient;

        internal Watch.WatchClient _watchClient;

        internal Lease.LeaseClient _leaseClient;

        internal Lock.LockClient _lockClient;

        internal Cluster.ClusterClient _clusterClient;

        internal Maintenance.MaintenanceClient _maintenanceClient;

        internal Auth.AuthClient _authClient;

        private Func<CallInvoker> createNewCallInvoker;

        public Connection(Uri node, GrpcChannelOptions? grpcChannelOptions, params Interceptor[] interceptors)
        {
            createNewCallInvoker = () =>
            {
                GrpcChannel channel;
// #if NET5_0 || NET6_0
//                 if (handlerOptions != null)
//                 {
//                     handler = new SocketsHttpHandler()
//                     {
//                         KeepAlivePingDelay = handlerOptions.KeepAlivePingDelay,
//                         KeepAlivePingTimeout = handlerOptions.KeepAlivePingTimeout,
//                         ConnectTimeout = handlerOptions.ConnectTimeout,
//                         EnableMultipleHttp2Connections = handlerOptions.EnableMultipleHttp2Connections,
//                         KeepAlivePingPolicy =
//                             handlerOptions.KeepAlivePingPolicyWithActiveRequests
//                                 ? HttpKeepAlivePingPolicy.WithActiveRequests
//                                 : HttpKeepAlivePingPolicy.Always,
//                     };
//                 }
// #endif
#if NETCOREAPP3_1 || NETCOREAPP3_0
                    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
#endif

                channel = grpcChannelOptions == null
                    ? GrpcChannel.ForAddress(node)
                    : GrpcChannel.ForAddress(
                        node,
                        grpcChannelOptions);


                CallInvoker callInvoker;

                if (interceptors !=null && interceptors.Length > 0)
                {
                    callInvoker = channel.Intercept(interceptors);
                }
                else
                {
                    callInvoker = channel.CreateCallInvoker();
                }

                return callInvoker;
            };

            RecreateClients();

        }

        internal void RecreateClients()
        {
            var callInvoker = createNewCallInvoker();
            _kvClient = new KV.KVClient(callInvoker);
            _watchClient = new Watch.WatchClient(callInvoker);
            _leaseClient = new Lease.LeaseClient(callInvoker);
            _lockClient = new V3Lockpb.Lock.LockClient(callInvoker);
            _clusterClient = new Cluster.ClusterClient(callInvoker);
            _maintenanceClient = new Maintenance.MaintenanceClient(callInvoker);
            _authClient = new Auth.AuthClient(callInvoker);
        }
    }
}
