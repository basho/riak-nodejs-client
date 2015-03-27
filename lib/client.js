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

/*
 * KV methods
 */

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

/**
 * @method DeleteValue
 */
Client.prototype.DeleteValue = function(options) {
    var cmd = new DeleteValue(options);
    this.cluster.execute(cmd);
};

/**
 * @method FetchBucketProps
 */
Client.prototype.FetchBucketProps = function(options) {
    var cmd = new FetchBucketProps(options);
    this.cluster.execute(cmd);
};

/**
 * @method FetchValue
 */
Client.prototype.FetchValue = function(options) {
    var cmd = new FetchValue(options);
    this.cluster.execute(cmd);
};

/**
 * @method ListBuckets
 */
Client.prototype.ListBuckets = function(options) {
    var cmd = new ListBuckets(options);
    this.cluster.execute(cmd);
};

/**
 * @method ListKeys
 */
Client.prototype.ListKeys = function(options) {
    var cmd = new ListKeys(options);
    this.cluster.execute(cmd);
};

/**
 * @method SecondaryIndexQuery
 */
Client.prototype.SecondaryIndexQuery = function(options) {
    var cmd = new SecondaryIndexQuery(options);
    this.cluster.execute(cmd);
};

/**
 * @method StoreBucketProps
 */
Client.prototype.StoreBucketProps = function(options) {
    var cmd = new StoreBucketProps(options);
    this.cluster.execute(cmd);
};

/**
 * @method StoreValue
 */
Client.prototype.StoreValue = function(options) {
    var cmd = new StoreValue(options);
    this.cluster.execute(cmd);
};

/*
 * CRDT methods
 */

/**
 * @method FetchSet
 */
Client.prototype.FetchSet = function(options) {
    var cmd = new FetchSet(options);
    this.cluster.execute(cmd);
};

/*
 * YZ methods
 */
/**
 * @method DeleteIndex
 */
Client.prototype.DeleteIndex = function(options) {
    var cmd = new DeleteIndex(options);
    this.cluster.execute(cmd);
};

/**
 * @method FetchIndex
 */
Client.prototype.FetchIndex = function(options) {
    var cmd = new FetchIndex(options);
    this.cluster.execute(cmd);
};

/**
 * @method FetchSchema
 */
Client.prototype.FetchSchema = function(options) {
    var cmd = new FetchSchema(options);
    this.cluster.execute(cmd);
};

/**
 * @method Search
 */
Client.prototype.Search = function(options) {
    var cmd = new Search(options);
    this.cluster.execute(cmd);
};

/**
 * @method StoreIndex
 */
Client.prototype.StoreIndex = function(options) {
    var cmd = new StoreIndex(options);
    this.cluster.execute(cmd);
};

/**
 * @method StoreSchema
 */
Client.prototype.StoreSchema = function(options) {
    var cmd = new StoreSchema(options);
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

// KV commands
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

