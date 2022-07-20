// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Etcdserverpb;

namespace dotnet_etcd
{
    public class LeaseKeeper
    {
        private readonly Channel<LeaseKeepAliveRequest> _keepAliveRequestChannel;
        private readonly Channel<LeaseKeepAliveResponse> _keepAliveResponseChannel;

        public long Id { get; private set; }

        public LeaseKeeper(long id, Channel<LeaseKeepAliveRequest> keepAliveRequestChannel)
        {
            _keepAliveRequestChannel = keepAliveRequestChannel;
            _keepAliveResponseChannel = Channel.CreateUnbounded<LeaseKeepAliveResponse>();
            Id = id;
        }

        public async Task StartKeepAliveAsync(CancellationToken cancellationToken)
        {
            если время лизы истечет, а респонса не было
            await _keepAliveRequestChannel.Writer.WriteAsync(
                new LeaseKeepAliveRequest(){ID = Id},
                cancellationToken);
            await foreach (var kaRsp in _keepAliveResponseChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                if (kaRsp.ID != Id  //как такое может быть? воспроизвести немедля. если это notfound то надо кидать эксепшен
                    || kaRsp.TTL == 0) // expired or not found
                {
                    return;
                }
                await Task.Delay(
                    TimeSpan.FromMilliseconds( kaRsp.TTL/ 3.0 * 1000),
                    cancellationToken).ConfigureAwait(false);
                await _keepAliveRequestChannel.Writer.WriteAsync(
                    new LeaseKeepAliveRequest() { ID = Id },
                    cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task KeepAliveResponseReceived(LeaseKeepAliveResponse rsp)
        {
            await _keepAliveResponseChannel.Writer.WriteAsync(rsp).ConfigureAwait(false);
        }

    }
}
