/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Core modules
var RiakNode = require('./core/riaknode');
var RiakCluster = require('./core/riakcluster');

// Command modules
var Ping = require('./commands/ping');

// KV commands
var DeleteValue = require('./commands/kv/deletevalue');
var FetchBucketProps = require('./commands/kv/fetchbucketprops');
var FetchValue = require('./commands/kv/fetchvalue');
var ListBuckets = require('./commands/kv/listbuckets');
var ListKeys = require('./commands/kv/listkeys');
var RiakObject = require('./commands/kv/riakobject');
var SecondaryIndexQuery = require('./commands/kv/secondaryindexquery');
var StoreBucketProps = require('./commands/kv/storebucketprops');
var StoreValue = require('./commands/kv/storevalue');

// CRDT commands
var FetchSet = require('./commands/crdt/fetchset');

// Yokozuna commands
var DeleteIndex = require('./commands/yokozuna/deleteindex');
var FetchIndex = require('./commands/yokozuna/fetchindex');
var FetchSchema = require('./commands/yokozuna/fetchschema');
var Search = require('./commands/yokozuna/search');
var StoreIndex = require('./commands/yokozuna/storeindex');
var StoreSchema = require('./commands/yokozuna/storeschema');

/**
 * Provides the Client class
 * @module Client
 */

/**
 * A Class that represents a Riak client.
 *
 * @class Client
 * @constructor
 * @param {Object} cluster - the cluster to use.
 */
function Client(cluster) {
    this.cluster = cluster;
    this.cluster.start();
}

/**
 * @method execute
 */
Client.prototype.execute = function(command) {
    this.cluster.execute(command);
};

/**
 * @method ping
 */
Client.prototype.ping = function(callback) {
    var cmd = new Ping(callback);
    this.cluster.execute(cmd);
};

/*
 * Poor man's namespaces
 */
function Riak() { }
function Commands() { }
function KV() { }
function CRDT() { }
function YZ() { }

// Core exports
module.exports = Riak;
module.exports.Client = Client;

module.exports.Node = RiakNode;
module.exports.Node.buildNodes = RiakNode.buildNodes;
module.exports.Node.Builder = RiakNode.Builder;

module.exports.Cluster = RiakCluster;
module.exports.Cluster.Builder = RiakCluster.Builder;

// Command exports
module.exports.Commands = Commands;
module.exports.Commands.Ping = Ping;

module.exports.Commands.KV = KV;
module.exports.Commands.CRDT = CRDT;
module.exports.Commands.YZ = YZ;

module.exports.Commands.KV.DeleteValue = DeleteValue;
module.exports.Commands.KV.FetchBucketProps = FetchBucketProps;
module.exports.Commands.KV.FetchValue = FetchValue;
module.exports.Commands.KV.ListBuckets = ListBuckets;
module.exports.Commands.KV.ListKeys = ListKeys;
module.exports.Commands.KV.RiakObject = RiakObject;
module.exports.Commands.KV.SecondaryIndexQuery = SecondaryIndexQuery;
module.exports.Commands.KV.StoreBucketProps = StoreBucketProps;
module.exports.Commands.KV.StoreValue = StoreValue;

// CRDT commands
module.exports.Commands.CRDT.FetchSet = FetchSet;

// Yokozuna commands
module.exports.Commands.YZ.DeleteIndex = DeleteIndex;
module.exports.Commands.YZ.FetchIndex = FetchIndex;
module.exports.Commands.YZ.FetchSchema = FetchSchema;
module.exports.Commands.YZ.Search = Search;
module.exports.Commands.YZ.StoreIndex = StoreIndex;
module.exports.Commands.YZ.StoreSchema = StoreSchema;

