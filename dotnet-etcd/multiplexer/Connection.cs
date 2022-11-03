// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Net.Http;
using Etcdserverpb;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
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

        public Connection(Uri node, HttpMessageHandler handler = null, bool ssl = false,
            bool useLegacyRpcExceptionForCancellation = false, SocketsHttpHandlerOptions handlerOptions = null, params Interceptor[] interceptors)
        {
            createNewCallInvoker = () =>
            {
                GrpcChannel channel;
#if NET5_0 || NET6_0
                if (handlerOptions != null)
                {
                    handler = new SocketsHttpHandler()
                    {
                        KeepAlivePingDelay = handlerOptions.KeepAlivePingDelay,
                        KeepAlivePingTimeout = handlerOptions.KeepAlivePingTimeout,
                        ConnectTimeout = handlerOptions.ConnectTimeout,
                        EnableMultipleHttp2Connections = handlerOptions.EnableMultipleHttp2Connections,
                        KeepAlivePingPolicy =
                            handlerOptions.KeepAlivePingPolicyWithActiveRequests
                                ? HttpKeepAlivePingPolicy.WithActiveRequests
                                : HttpKeepAlivePingPolicy.Always,
                    };
                }
#endif
                if (ssl)
                {
                    channel = GrpcChannel.ForAddress(node, new GrpcChannelOptions
                    {
                        Credentials = new SslCredentials(),
                        HttpHandler = handler,
                        ThrowOperationCanceledOnCancellation = !useLegacyRpcExceptionForCancellation,
                        MaxReceiveMessageSize = null,
                    });
                }
                else
                {
#if NETCOREAPP3_1 || NETCOREAPP3_0
                    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
#endif
                    var options = new GrpcChannelOptions
                    {
                        Credentials = ChannelCredentials.Insecure,
                        HttpHandler = handler,
                        ThrowOperationCanceledOnCancellation = !useLegacyRpcExceptionForCancellation,
                        MaxReceiveMessageSize = null,
                    };

                    channel = GrpcChannel.ForAddress(node, options);
                }

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
