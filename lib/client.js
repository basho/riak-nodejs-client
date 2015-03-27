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
 * Ping a node in the cluster
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

