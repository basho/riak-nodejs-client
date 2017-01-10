/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var Riak = require('../../index.js');
var assert = require('assert');

describe('Client', function() {

    describe('ctor', function() {
        it('Riak.Client requires cluster argument', function(done) {
            assert.throws(function () {
                var c = new Riak.Client();
            });
            done();
        });
    });

    describe('export validation', function() {
        it('Riak.Node', function(done) {
            var typeofRiakNode = typeof(Riak.Node);
            assert(typeofRiakNode === 'function', "typeof(Riak.Node): " + typeofRiakNode);
            done();
        });
        it('Riak.Node.Builder', function(done) {
            var typeofRiakNode = typeof(Riak.Node.Builder);
            assert(typeofRiakNode === 'function', "typeof(Riak.Node.Builder): " + typeofRiakNode);
            done();
        });
        it('Riak.Node.State', function(done) {
            var typeofRiakNode = typeof(Riak.Node.State);
            assert(typeofRiakNode === 'object', "typeof(Riak.Node.State): " + typeofRiakNode);
            done();
        });
        it('Riak.Node.buildNodes', function(done) {
            var typeofRiakNode = typeof(Riak.Node.buildNodes);
            assert(typeofRiakNode === 'function', "typeof(Riak.Node.buildNodes): " + typeofRiakNode);
            done();
        });

        it('Riak.Client', function(done) {
            var typeofRiakClient = typeof(Riak.Client);
            assert(typeofRiakClient === 'function', "typeof(Riak.Client): " + typeofRiakClient);
            done();
        });

        it('Riak.Cluster', function(done) {
            var typeofRiakCluster = typeof(Riak.Cluster);
            assert(typeofRiakCluster === 'function', "typeof(Riak.Cluster): " + typeofRiakCluster);
            done();
        });
        it('Riak.Cluster.State', function(done) {
            var typeofRiakClusterState = typeof(Riak.Cluster.State);
            assert(typeofRiakClusterState === 'object', "typeof(Riak.Cluster.State): " + typeofRiakClusterState);
            done();
        });
        it('Riak.Cluster.DefaultNodeManager', function(done) {
            var typeofRiakClusterDefaultNodeManager = typeof(Riak.Cluster.DefaultNodeManager);
            assert(typeofRiakClusterDefaultNodeManager === 'function', "typeof(Riak.Cluster.DefaultNodeManager): " + typeofRiakClusterDefaultNodeManager);
            done();
        });
        it('Riak.Cluster.RoundRobinNodeManager', function(done) {
            var typeofRiakClusterRoundRobinNodeManager = typeof(Riak.Cluster.RoundRobinNodeManager);
            assert(typeofRiakClusterRoundRobinNodeManager === 'function', "typeof(Riak.Cluster.RoundRobinNodeManager): " + typeofRiakClusterRoundRobinNodeManager);
            done();
        });
        it('Riak.Cluster.LeastExecutingNodeManager', function(done) {
            var typeofRiakClusterLeastExecutingNodeManager = typeof(Riak.Cluster.LeastExecutingNodeManager);
            assert(typeofRiakClusterLeastExecutingNodeManager === 'function', "typeof(Riak.Cluster.LeastExecutingNodeManager): " + typeofRiakClusterLeastExecutingNodeManager);
            done();
        });

        it('Riak.Commands.Ping', function(done) {
            var typeofCommandsPing = typeof(Riak.Commands.Ping);
            assert(typeofCommandsPing === 'function', "typeof(Riak.Commands.Ping): " + typeofCommandsPing);
            done();
        });

        it('Riak.Commands.TS.Store', function(done) {
            var typeofTsStore = typeof(Riak.Commands.TS.Store);
            assert(typeofTsStore === 'function', "typeof(Riak.Commands.TS.Store): " + typeofTsStore);
            done();
        });
        it('Riak.Commands.TS.Describe', function(done) {
            var typeofTsDescribe = typeof(Riak.Commands.TS.Describe);
            assert(typeofTsDescribe === 'function', "typeof(Riak.Commands.TS.Describe): " + typeofTsDescribe);
            done();
        });
        it('Riak.Commands.TS.Query', function(done) {
            var typeofTsQuery = typeof(Riak.Commands.TS.Query);
            assert(typeofTsQuery === 'function', "typeof(Riak.Commands.TS.Query): " + typeofTsQuery);
            done();
        });
        it('Riak.Commands.TS.Get', function(done) {
            var typeofTsGet = typeof(Riak.Commands.TS.Get);
            assert(typeofTsGet === 'function', "typeof(Riak.Commands.TS.Get): " + typeofTsGet);
            done();
        });
        it('Riak.Commands.TS.Delete', function(done) {
            var typeofTsDelete = typeof(Riak.Commands.TS.Delete);
            assert(typeofTsDelete === 'function', "typeof(Riak.Commands.TS.Delete): " + typeofTsDelete);
            done();
        });

        it('Riak.Commands.TS.ColumnType', function(done) {
            var typeofTsColumnType = typeof(Riak.Commands.TS.ColumnType);
            assert(typeofTsColumnType === 'object', "typeof(Riak.Commands.TS.ColumnType): " + typeofTsColumnType);
            done();
        });

        it('Riak.Commands.KV.DeleteValue', function(done) {
            var typeofKvDeleteValue = typeof(Riak.Commands.KV.DeleteValue);
            assert(typeofKvDeleteValue === 'function', "typeof(Riak.Commands.KV.DeleteValue): " + typeofKvDeleteValue);
            done();
        });
        it('Riak.Commands.KV.FetchBucketProps', function(done) {
            var typeofKvFetchBucketProps = typeof(Riak.Commands.KV.FetchBucketProps);
            assert(typeofKvFetchBucketProps === 'function', "typeof(Riak.Commands.KV.FetchBucketProps): " + typeofKvFetchBucketProps);
            done();
        });
        it('Riak.Commands.KV.FetchBucketTypeProps', function(done) {
            var typeofKvFetchBucketTypeProps = typeof(Riak.Commands.KV.FetchBucketTypeProps);
            assert(typeofKvFetchBucketTypeProps === 'function', "typeof(Riak.Commands.KV.FetchBucketTypeProps): " + typeofKvFetchBucketTypeProps);
            done();
        });
        it('Riak.Commands.KV.FetchValue', function(done) {
            var typeofKvFetchValue = typeof(Riak.Commands.KV.FetchValue);
            assert(typeofKvFetchValue === 'function', "typeof(Riak.Commands.KV.FetchValue): " + typeofKvFetchValue);
            done();
        });
        it('Riak.Commands.KV.ListBuckets', function(done) {
            var typeofKvListBuckets = typeof(Riak.Commands.KV.ListBuckets);
            assert(typeofKvListBuckets === 'function', "typeof(Riak.Commands.KV.ListBuckets): " + typeofKvListBuckets);
            done();
        });
        it('Riak.Commands.KV.ListKeys', function(done) {
            var typeofKvListKeys = typeof(Riak.Commands.KV.ListKeys);
            assert(typeofKvListKeys === 'function', "typeof(Riak.Commands.KV.ListKeys): " + typeofKvListKeys);
            done();
        });
        it('Riak.Commands.KV.RiakObject', function(done) {
            var typeofKvRiakObject = typeof(Riak.Commands.KV.RiakObject);
            assert(typeofKvRiakObject === 'function', "typeof(Riak.Commands.KV.RiakObject): " + typeofKvRiakObject);
            done();
        });
        it('Riak.Commands.KV.SecondaryIndexQuery', function(done) {
            var typeofKvSecondaryIndexQuery = typeof(Riak.Commands.KV.SecondaryIndexQuery);
            assert(typeofKvSecondaryIndexQuery === 'function', "typeof(Riak.Commands.KV.SecondaryIndexQuery): " + typeofKvSecondaryIndexQuery);
            done();
        });
        it('Riak.Commands.KV.StoreBucketProps', function(done) {
            var typeofKvStoreBucketProps = typeof(Riak.Commands.KV.StoreBucketProps);
            assert(typeofKvStoreBucketProps === 'function', "typeof(Riak.Commands.KV.StoreBucketProps): " + typeofKvStoreBucketProps);
            done();
        });
        it('Riak.Commands.KV.ResetBucketProps', function(done) {
            var typeofKvResetBucketProps = typeof(Riak.Commands.KV.ResetBucketProps);
            assert(typeofKvResetBucketProps === 'function', "typeof(Riak.Commands.KV.ResetBucketProps): " + typeofKvResetBucketProps);
            done();
        });
        it('Riak.Commands.KV.StoreBucketTypeProps', function(done) {
            var typeofKvStoreBucketTypeProps = typeof(Riak.Commands.KV.StoreBucketTypeProps);
            assert(typeofKvStoreBucketTypeProps === 'function', "typeof(Riak.Commands.KV.StoreBucketTypeProps): " + typeofKvStoreBucketTypeProps);
            done();
        });
        it('Riak.Commands.KV.StoreValue', function(done) {
            var typeofKvStoreValue = typeof(Riak.Commands.KV.StoreValue);
            assert(typeofKvStoreValue === 'function', "typeof(Riak.Commands.KV.StoreValue): " + typeofKvStoreValue);
            done();
        });
        it('Riak.Commands.KV.FetchPreflist', function(done) {
            var typeofKvFetchPreflist = typeof(Riak.Commands.KV.FetchPreflist);
            assert(typeofKvFetchPreflist === 'function', "typeof(Riak.Commands.KV.FetchPreflist): " + typeofKvFetchPreflist);
            done();
        });

        it('Riak.Commands.CRDT.FetchSet', function(done) {
            var typeofCrdtFetchSet = typeof(Riak.Commands.CRDT.FetchSet);
            assert(typeofCrdtFetchSet === 'function', "typeof(Riak.Commands.CRDT.FetchSet): " + typeofCrdtFetchSet);
            done();
        });

        it('Riak.Commands.CRDT.UpdateSet', function(done) {
            var typeofCrdtUpdateSet = typeof(Riak.Commands.CRDT.UpdateSet);
            assert(typeofCrdtUpdateSet === 'function', "typeof(Riak.Commands.CRDT.UpdateSet): " + typeofCrdtUpdateSet);
            done();
        });
        
        it('Riak.Commands.CRDT.FetchCounter', function(done) {
            var typeofCrdtFetchCounter = typeof(Riak.Commands.CRDT.FetchCounter);
            assert(typeofCrdtFetchCounter === 'function', "typeof(Riak.Commands.CRDT.FetchCounter): " + typeofCrdtFetchCounter);
            done();
        });
        
        it('Riak.Commands.CRDT.UpdateCounter', function(done) {
            var typeofCrdtUpdateCounter = typeof(Riak.Commands.CRDT.UpdateCounter);
            assert(typeofCrdtUpdateCounter === 'function', "typeof(Riak.Commands.CRDT.UpdateCounter): " + typeofCrdtUpdateCounter);
            done();
        });
        
        it('Riak.Commands.CRDT.FetchMap', function(done) {
            var typeofCrdtFetchMap = typeof(Riak.Commands.CRDT.FetchMap);
            assert(typeofCrdtFetchMap === 'function', "typeof(Riak.Commands.CRDT.FetchMap): " + typeofCrdtFetchMap);
            done();
        });
        
        it('Riak.Commands.CRDT.UpdateMap', function(done) {
            var typeofCrdtUpdateMap = typeof(Riak.Commands.CRDT.UpdateMap);
            assert(typeofCrdtUpdateMap === 'function', "typeof(Riak.Commands.CRDT.UpdateMap): " + typeofCrdtUpdateMap);
            done();
        });

        it('Riak.Commands.YZ.DeleteIndex', function(done) {
            var typeofYzDeleteIndex = typeof(Riak.Commands.YZ.DeleteIndex);
            assert(typeofYzDeleteIndex === 'function', "typeof(Riak.Commands.YZ.DeleteIndex): " + typeofYzDeleteIndex);
            done();
        });
        it('Riak.Commands.YZ.FetchIndex', function(done) {
            var typeofYzFetchIndex = typeof(Riak.Commands.YZ.FetchIndex);
            assert(typeofYzFetchIndex === 'function', "typeof(Riak.Commands.YZ.FetchIndex): " + typeofYzFetchIndex);
            done();
        });
        it('Riak.Commands.YZ.FetchSchema', function(done) {
            var typeofYzFetchSchema = typeof(Riak.Commands.YZ.FetchSchema);
            assert(typeofYzFetchSchema === 'function', "typeof(Riak.Commands.YZ.FetchSchema): " + typeofYzFetchSchema);
            done();
        });
        it('Riak.Commands.YZ.Search', function(done) {
            var typeofYzSearch = typeof(Riak.Commands.YZ.Search);
            assert(typeofYzSearch === 'function', "typeof(Riak.Commands.YZ.Search): " + typeofYzSearch);
            done();
        });
        it('Riak.Commands.YZ.StoreIndex', function(done) {
            var typeofYzStoreIndex = typeof(Riak.Commands.YZ.StoreIndex);
            assert(typeofYzStoreIndex === 'function', "typeof(Riak.Commands.YZ.StoreIndex): " + typeofYzStoreIndex);
            done();
        });
        it('Riak.Commands.YZ.StoreSchema', function(done) {
            var typeofYzStoreSchema = typeof(Riak.Commands.YZ.StoreSchema);
            assert(typeofYzStoreSchema === 'function', "typeof(Riak.Commands.YZ.StoreSchema): " + typeofYzStoreSchema);
            done();
        });
        
        it('Riak.Commands.MR.MapReduce', function(done) {
            var typeofMrMapReduce = typeof(Riak.Commands.MR.MapReduce);
            assert(typeofMrMapReduce === 'function', "typeof(Riak.Commands.MR.MapReduce): " + typeofMrMapReduce);
            done();
        });
    });
});

