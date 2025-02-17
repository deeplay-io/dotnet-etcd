﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Etcdserverpb;

using V3Lockpb;

namespace dotnet_etcd.interfaces
{
    public interface IEtcdClient
    {
        AlarmResponse Alarm(AlarmRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AlarmResponse> AlarmAsync(AlarmRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthDisableResponse AuthDisable(AuthDisableRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthDisableResponse> AuthDisableAsync(AuthDisableRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthEnableResponse AuthEnable(AuthEnableRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthEnableResponse> AuthEnableAsync(AuthEnableRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthenticateResponse Authenticate(AuthenticateRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthenticateResponse> AuthenticateAsync(AuthenticateRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        CompactionResponse Compact(CompactionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<CompactionResponse> CompactAsync(CompactionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        DefragmentResponse Defragment(DefragmentRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<DefragmentResponse> DefragmentAsync(DefragmentRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        DeleteRangeResponse Delete(DeleteRangeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        DeleteRangeResponse Delete(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<DeleteRangeResponse> DeleteAsync(DeleteRangeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<DeleteRangeResponse> DeleteAsync(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        DeleteRangeResponse DeleteRange(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<DeleteRangeResponse> DeleteRangeAsync(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Dispose();
        RangeResponse Get(RangeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        RangeResponse Get(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<RangeResponse> GetAsync(RangeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<RangeResponse> GetAsync(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        RangeResponse GetRange(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<RangeResponse> GetRangeAsync(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        IDictionary<string, string> GetRangeVal(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<IDictionary<string, string>> GetRangeValAsync(string prefixKey, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        string GetVal(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<string> GetValAsync(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        HashResponse Hash(HashRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<HashResponse> HashAsync(HashRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        HashKVResponse HashKV(HashKVRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<HashKVResponse> HashKVAsync(HashKVRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        LeaseGrantResponse LeaseGrant(LeaseGrantRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<LeaseGrantResponse> LeaseGrantAsync(LeaseGrantRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task LeaseKeepAlive(LeaseKeepAliveRequest request, Action<LeaseKeepAliveResponse> method, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null);
        Task LeaseKeepAlive(LeaseKeepAliveRequest request, Action<LeaseKeepAliveResponse>[] methods, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null);
        Task LeaseKeepAlive(LeaseKeepAliveRequest[] requests, Action<LeaseKeepAliveResponse> method, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null);
        Task LeaseKeepAlive(LeaseKeepAliveRequest[] requests, Action<LeaseKeepAliveResponse>[] methods, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null, DateTime? deadline = null);
        Task LeaseKeepAlive(long leaseId, CancellationToken cancellationToken);
        LeaseRevokeResponse LeaseRevoke(LeaseRevokeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<LeaseRevokeResponse> LeaseRevokeAsync(LeaseRevokeRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        LeaseTimeToLiveResponse LeaseTimeToLive(LeaseTimeToLiveRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<LeaseTimeToLiveResponse> LeaseTimeToLiveAsync(LeaseTimeToLiveRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        LockResponse Lock(LockRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        LockResponse Lock(string name, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<LockResponse> LockAsync(LockRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<LockResponse> LockAsync(string name, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        MemberAddResponse MemberAdd(MemberAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<MemberAddResponse> MemberAddAsync(MemberAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        MemberListResponse MemberList(MemberListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<MemberListResponse> MemberListAsync(MemberListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        MemberRemoveResponse MemberRemove(MemberRemoveRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<MemberRemoveResponse> MemberRemoveAsync(MemberRemoveRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        MemberUpdateResponse MemberUpdate(MemberUpdateRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<MemberUpdateResponse> MemberUpdateAsync(MemberUpdateRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        MoveLeaderResponse MoveLeader(MoveLeaderRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<MoveLeaderResponse> MoveLeaderAsync(MoveLeaderRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        PutResponse Put(PutRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        PutResponse Put(string key, string val, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<PutResponse> PutAsync(PutRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<PutResponse> PutAsync(string key, string val, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleAddResponse RoleAdd(AuthRoleAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleAddResponse> RoleAddAsync(AuthRoleAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleDeleteResponse RoleDelete(AuthRoleDeleteRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleDeleteResponse> RoleDeleteAsync(AuthRoleDeleteRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleGetResponse RoleGet(AuthRoleGetRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleGetResponse> RoleGetASync(AuthRoleGetRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleGrantPermissionResponse RoleGrantPermission(AuthRoleGrantPermissionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleGrantPermissionResponse> RoleGrantPermissionAsync(AuthRoleGrantPermissionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleListResponse RoleList(AuthRoleListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleListResponse> RoleListAsync(AuthRoleListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthRoleRevokePermissionResponse RoleRevokePermission(AuthRoleRevokePermissionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthRoleRevokePermissionResponse> RoleRevokePermissionAsync(AuthRoleRevokePermissionRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Snapshot(SnapshotRequest request, Action<SnapshotResponse> method, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null, DateTime? deadline = null);
        Task Snapshot(SnapshotRequest request, Action<SnapshotResponse>[] methods, CancellationToken cancellationToken, Grpc.Core.Metadata headers = null, DateTime? deadline = null);
        StatusResponse Status(StatusRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<StatusResponse> StatusASync(StatusRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        TxnResponse Transaction(TxnRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<TxnResponse> TransactionAsync(TxnRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        UnlockResponse Unlock(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        UnlockResponse Unlock(UnlockRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<UnlockResponse> UnlockAsync(string key, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<UnlockResponse> UnlockAsync(UnlockRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserAddResponse UserAdd(AuthUserAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserAddResponse> UserAddAsync(AuthUserAddRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserChangePasswordResponse UserChangePassword(AuthUserChangePasswordRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserChangePasswordResponse> UserChangePasswordAsync(AuthUserChangePasswordRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserDeleteResponse UserDelete(AuthUserDeleteRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserDeleteResponse> UserDeleteAsync(AuthUserDeleteRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserGetResponse UserGet(AuthUserGetRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserGetResponse> UserGetAsync(AuthUserGetRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserGrantRoleResponse UserGrantRole(AuthUserGrantRoleRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserGrantRoleResponse> UserGrantRoleAsync(AuthUserGrantRoleRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserListResponse UserList(AuthUserListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserListResponse> UserListAsync(AuthUserListRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        AuthUserRevokeRoleResponse UserRevokeRole(AuthUserRevokeRoleRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task<AuthUserRevokeRoleResponse> UserRevokeRoleAsync(AuthUserRevokeRoleRequest request, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string key, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string key, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string key, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string key, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string[] keys, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string[] keys, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string[] keys, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void Watch(string[] keys, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest request, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest request, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest request, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest request, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest[] requests, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest[] requests, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest[] requests, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task Watch(WatchRequest[] requests, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string path, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string path, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string path, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string path, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string[] paths, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string[] paths, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string[] paths, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        void WatchRange(string[] paths, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest request, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest request, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest request, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest request, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest[] requests, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest[] requests, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest[] requests, Action<WatchResponse> method, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
        Task WatchRange(WatchRequest[] requests, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null, DateTime? deadline = null, CancellationToken cancellationToken = default);
    }
}