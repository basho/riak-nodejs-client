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

var RiakNode = require('./core/riaknode');
var RiakCluster = require('./core/riakcluster');
var Ping = require('./commands/ping');

function Client(cluster) {
    this.cluster = cluster;
}

Client.prototype.ping = function(callback) {
    var cmd = new Ping(callback);
    this.cluster.execute(cmd);
};

/*
 * Poor man's namespaces
 */
function Riak() { }
function Commands() { }

module.exports = Riak;
module.exports.Client = Client;
module.exports.Node = RiakNode;
module.exports.Cluster = RiakCluster;
module.exports.Commands = Commands;
module.exports.Commands.Ping = Ping;

