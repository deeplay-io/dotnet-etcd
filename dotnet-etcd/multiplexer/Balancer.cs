// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;

using Etcdserverpb;

using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace dotnet_etcd.multiplexer
{

    internal class Balancer
    {
        internal readonly HashSet<Connection> _healthyNode;

        /// <summary>
        /// No of etcd nodes
        /// </summary>
        internal readonly int _numNodes;

        /// <summary>
        /// Last used node index
        /// </summary>
        private int _lastNodeIndex;

        /// <summary>
        /// Random object for randomizing selected node
        /// </summary>
        private static readonly Random s_random = new Random();

        internal Balancer(List<Uri> nodes, GrpcChannelOptions? grpcChannelOptions,
            params Interceptor[] interceptors)
        {
            _numNodes = nodes.Count;
            _lastNodeIndex = s_random.Next(-1, _numNodes);

            _healthyNode = new HashSet<Connection>();

            foreach (Uri node in nodes)
            {
                Connection connection = new Connection(node, grpcChannelOptions, interceptors);
                _healthyNode.Add(connection);
            }
        }

        internal Connection GetConnection() => _healthyNode.ElementAt(GetNextNodeIndex());

        internal Connection GetConnection(int index) => _healthyNode.ElementAt(index);

        internal int GetNextNodeIndex()
        {
            int initial, computed;
            do
            {
                initial = _lastNodeIndex;
                computed = initial + 1;
                computed = computed >= _numNodes ? computed = 0 : computed;
            }
            while (Interlocked.CompareExchange(ref _lastNodeIndex, computed, initial) != initial);
            return computed;
        }


    }
}
