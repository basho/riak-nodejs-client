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
var Core = require('./core/core');

// Ping command
var Ping = require('./commands/ping');

// KV exports / commands
var KV = require('./commands/kv');

// CRDT exports / commands
var CRDT = require('./commands/crdt');

// Yokozuna exports / commands
var YZ = require('./commands/yz');

/**
 * Provides the Client class
 * @module Client
 */

/**
 * A Class that represents a Riak client.
 *
 * @class Client
 * @constructor
 * @param {Object} cluster - the cluster to use. See {{#crossLink "RiakCluster"}}{{/crossLink}}.
 */
function Client(cluster) {
    if (cluster) {
        this.cluster = cluster;
        this.cluster.start();
    } else {
        throw new Error('cluster argument is required and should be an instance of Riak.Cluster');
    }
}

/*
 * KV methods
 */

/**
 * See {{#crossLink "RiakCluster/execute:method"}}RiakCluster#execute{{/crossLink}}
 * @method execute
 */
Client.prototype.execute = function(command) {
    this.cluster.execute(command);
};

/**
 * See {{#crossLink "Ping"}}{{/crossLink}}
 * @method ping
 */
Client.prototype.ping = function(callback) {
    var cmd = new Ping(callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "DeleteValue"}}{{/crossLink}}
 * @method DeleteValue
 */
Client.prototype.DeleteValue = function(options) {
    var cmd = new KV.DeleteValue(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchBucketProps"}}{{/crossLink}}
 * @method FetchBucketProps
 */
Client.prototype.FetchBucketProps = function(options) {
    var cmd = new KV.FetchBucketProps(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchValue"}}{{/crossLink}}
 * @method FetchValue
 */
Client.prototype.FetchValue = function(options) {
    var cmd = new KV.FetchValue(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListBuckets"}}{{/crossLink}}
 * @method ListBuckets
 */
Client.prototype.ListBuckets = function(options) {
    var cmd = new KV.ListBuckets(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListKeys"}}{{/crossLink}}
 * @method ListKeys
 */
Client.prototype.ListKeys = function(options) {
    var cmd = new KV.ListKeys(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "SecondaryIndexQuery"}}{{/crossLink}}
 * @method SecondaryIndexQuery
 */
Client.prototype.SecondaryIndexQuery = function(options) {
    var cmd = new KV.SecondaryIndexQuery(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreBucketProps"}}{{/crossLink}}
 * @method StoreBucketProps
 */
Client.prototype.StoreBucketProps = function(options) {
    var cmd = new KV.StoreBucketProps(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreValue"}}{{/crossLink}}
 * @method StoreValue
 */
Client.prototype.StoreValue = function(options) {
    var cmd = new KV.StoreValue(options);
    this.cluster.execute(cmd);
};

/*
 * CRDT methods
 */

/**
 * See {{#crossLink "FetchSet"}}{{/crossLink}}
 * @method FetchSet
 */
Client.prototype.FetchSet = function(options) {
    var cmd = new CRDT.FetchSet(options);
    this.cluster.execute(cmd);
};

/*
 * YZ methods
 */

/**
 * See {{#crossLink "DeleteIndex"}}{{/crossLink}}
 * @method DeleteIndex
 */
Client.prototype.DeleteIndex = function(options) {
    var cmd = new YZ.DeleteIndex(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchIndex"}}{{/crossLink}}
 * @method FetchIndex
 */
Client.prototype.FetchIndex = function(options) {
    var cmd = new YZ.FetchIndex(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchSchema"}}{{/crossLink}}
 * @method FetchSchema
 */
Client.prototype.FetchSchema = function(options) {
    var cmd = new YZ.FetchSchema(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Search"}}{{/crossLink}}
 * @method Search
 */
Client.prototype.Search = function(options) {
    var cmd = new YZ.Search(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreIndex"}}{{/crossLink}}
 * @method StoreIndex
 */
Client.prototype.StoreIndex = function(options) {
    var cmd = new YZ.StoreIndex(options);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreSchema"}}{{/crossLink}}
 * @method StoreSchema
 */
Client.prototype.StoreSchema = function(options) {
    var cmd = new YZ.StoreSchema(options);
    this.cluster.execute(cmd);
};

// Namespaces
function Riak() { }
function Commands() { }

// Core exports
module.exports = Riak;
module.exports.Client = Client;

module.exports.Node = Core.RiakNode;
module.exports.Node.buildNodes = Core.RiakNode.buildNodes;
module.exports.Node.Builder = Core.RiakNode.Builder;

module.exports.Cluster = Core.RiakCluster;
module.exports.Cluster.Builder = Core.RiakCluster.Builder;

// Command exports
module.exports.Commands = Commands;
module.exports.Commands.Ping = Ping;

module.exports.Commands.KV = KV;
module.exports.Commands.CRDT = CRDT;
module.exports.Commands.YZ = YZ;

