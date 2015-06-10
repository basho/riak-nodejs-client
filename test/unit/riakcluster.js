/*
 * Copyright 2014 Basho Technologies, Inc.
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

var RiakCluster = require('../../lib/core/riakcluster');
var RiakNode = require('../../lib/core/riaknode');
var assert = require('assert');

describe('RiakCluster', function() {
    describe('schema defaults', function() {
        it('should create new node manager per-cluster', function(done) {
            var cluster1 = new RiakCluster();
            var cluster2 = new RiakCluster();
            assert.notStrictEqual(cluster1.nodeManager, cluster2.nodeManager);
            done();
        });
    });
    describe('GitHub issues', function() {
        it('resolves GitHub Issue 64', function(done) {
            var nodeTemplate = new RiakNode.Builder().withMinConnections(10);
            var nodeAddys = ['192.168.1.1:8087', '192.168.1.2:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddys, nodeTemplate);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            assert.equal(myCluster.nodes.length, 2);
            myCluster.nodes.forEach(function (n) {
                n.state = RiakNode.State.RUNNING;
            });
            assert(myCluster.removeNode('192.168.1.1:8087'));
            assert.equal(myCluster.nodes.length, 1);
            assert.equal(myCluster.nodes[0].remoteAddress, '192.168.1.2');
            assert.equal(myCluster.nodes[0].remotePort, 8087);
            done();
        });
    });
});
