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

var kv = require('./kv');
var Riak = require('../lib/client');

var bucketType = 'no_siblings';
var bucket = 'kv_queue_benchmarks';

function buildClient(timeout) {
    var nodes = [];
    for (var i = 17; i <= 47; i += 10) {
        var port = 10000 + i;
        nodes.push(new Riak.Node({ remoteAddress: 'riak-test', remotePort: port.toString(), cork: true }));
    }
    var cluster = new Riak.Cluster.Builder()
        .withRiakNodes(nodes)
        .withQueueTimeout(timeout)
        .build();
    return new Riak.Client(cluster);
}

var client_500 = buildClient(500);
var client_125 = buildClient(125);
var client_50 = buildClient(50);

module.exports = {
    name: 'KV-Queue-Timeouts',
    tests: [
        {
            name: '500ms timeout',
            defer: true,
            fn: function (deferred) {
                var opts = {
                    bucketType: bucketType,
                    bucket: bucket,
                    client: client_500
                };
                kv(opts, deferred);
            }
        },
        {
            name: '125ms timeout',
            defer: true,
            fn: function (deferred) {
                var opts = {
                    bucketType: bucketType,
                    bucket: bucket,
                    client: client_125
                };
                kv(opts, deferred);
            }
        },
        {
            name: '50ms timeout',
            defer: true,
            fn: function (deferred) {
                var opts = {
                    bucketType: bucketType,
                    bucket: bucket,
                    client: client_50
                };
                kv(opts, deferred);
            }
        }
    ]
};
