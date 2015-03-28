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
 * @param {String[]|RiakCluster} cluster - either an array of IP|fqdn[:port] or the cluster to use. See {{#crossLink "RiakCluster"}}{{/crossLink}}.
 */
function Client() {
    if (arguments.length === 1 && arguments[0] instanceof Core.RiakCluster) {
        this.cluster = arguments[0];
        this.cluster.start();
    } else if (arguments.length) {
        var nodes = Core.RiakNode.buildNodes(arguments[0]);
        this.cluster = new Core.RiakCluster({ nodes: nodes});
        this.cluster.start();
    } else {
        throw new Error('an array of IP|fqdn[:port] or an instance of RiakCluser is required');
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
Client.prototype.deleteValue = function(options, callback) {
    var cmd = new KV.DeleteValue(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchBucketProps"}}{{/crossLink}}
 * @method FetchBucketProps
 */
Client.prototype.fetchBucketProps = function(options, callback) {
    var cmd = new KV.FetchBucketProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchValue"}}{{/crossLink}}
 * @method FetchValue
 */
Client.prototype.fetchValue = function(options, callback) {
    var cmd = new KV.FetchValue(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListBuckets"}}{{/crossLink}}
 * @method ListBuckets
 */
Client.prototype.listBuckets = function(options, callback) {
    var cmd = new KV.ListBuckets(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListKeys"}}{{/crossLink}}
 * @method ListKeys
 */
Client.prototype.listKeys = function(options, callback) {
    var cmd = new KV.ListKeys(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "SecondaryIndexQuery"}}{{/crossLink}}
 * @method SecondaryIndexQuery
 */
Client.prototype.secondaryIndexQuery = function(options, callback) {
    var cmd = new KV.SecondaryIndexQuery(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreBucketProps"}}{{/crossLink}}
 * @method StoreBucketProps
 */
Client.prototype.storeBucketProps = function(options, callback) {
    var cmd = new KV.StoreBucketProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreValue"}}{{/crossLink}}
 * @method StoreValue
 */
Client.prototype.storeValue = function(options, callback) {
    var cmd = new KV.StoreValue(options, callback);
    this.cluster.execute(cmd);
};

/*
 * CRDT methods
 */

/**
 * See {{#crossLink "FetchSet"}}{{/crossLink}}
 * @method FetchSet
 */
Client.prototype.fetchSet = function(options, callback) {
    var cmd = new CRDT.FetchSet(options, callback);
    this.cluster.execute(cmd);
};

/*
 * YZ methods
 */

/**
 * See {{#crossLink "DeleteIndex"}}{{/crossLink}}
 * @method DeleteIndex
 */
Client.prototype.deleteIndex = function(options, callback) {
    var cmd = new YZ.DeleteIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchIndex"}}{{/crossLink}}
 * @method FetchIndex
 */
Client.prototype.fetchIndex = function(options, callback) {
    var cmd = new YZ.FetchIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchSchema"}}{{/crossLink}}
 * @method FetchSchema
 */
Client.prototype.fetchSchema = function(options, callback) {
    var cmd = new YZ.FetchSchema(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Search"}}{{/crossLink}}
 * @method Search
 */
Client.prototype.search = function(options, callback) {
    var cmd = new YZ.Search(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreIndex"}}{{/crossLink}}
 * @method StoreIndex
 */
Client.prototype.storeIndex = function(options, callback) {
    var cmd = new YZ.StoreIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreSchema"}}{{/crossLink}}
 * @method StoreSchema
 */
Client.prototype.storeSchema = function(options, callback) {
    var cmd = new YZ.StoreSchema(options, callback);
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

