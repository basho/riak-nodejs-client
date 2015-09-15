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

var RiakCluster = require('../../../lib/core/riakcluster');
var RiakNode = require('../../../lib/core/riaknode');
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
    describe('Test getting a node index', function () {
        it('getting some nodes index', function (done) {
            var nodeAddrs = ['192.168.1.1:8087', '192.168.1.2:8087', '192.168.1.3:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddrs);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            var node = arrayOfNodes[0];
            assert.equal(myCluster.getNodeIndex(node),0);
            assert.equal(myCluster.getNodeIndex('192.168.1.2'),1);
            assert.equal(myCluster.getNodeIndex('192.168.1.3:8087'),2);
            assert.equal(myCluster.getNodeIndex('192.168.1.3:1234'),-1);
            assert.equal(myCluster.getNodeIndex('192.168.1.11'),-1);
            done();
        });
    });

    describe('Test removing node', function() {
        it('removing node while executing command should not throw exception', function(done) {
            var nodeTemplate = new RiakNode.Builder().withMinConnections(10);
            var nodeAddys = ['192.168.1.1:8087', '192.168.1.2:8087', '192.168.1.3:8087', '192.168.1.4:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddys, nodeTemplate);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            assert.equal(myCluster.nodes.length, 4);
            myCluster.nodes.forEach(function (n) {
                n.state = RiakNode.State.RUNNING;
                n.execute = function() {
                    return true;
                };
            });
            assert.equal(myCluster.nodeManager._nodeIndex, 0);
            myCluster.execute(myCluster.nodes, {onError: function() {}});
            assert.equal(myCluster.nodeManager._nodeIndex, 1);
            myCluster.execute(myCluster.nodes, {onError: function() {}});
            assert.equal(myCluster.nodeManager._nodeIndex, 2);

            assert(myCluster.removeNode('192.168.1.3:8087'));
            assert(myCluster.removeNode('192.168.1.4:8087'));
            assert.equal(myCluster.nodes.length, 2);
            assert.equal(myCluster.nodeManager._nodeIndex, 2);
            assert.equal(myCluster.nodes[0].remoteAddress, '192.168.1.1');
            assert.equal(myCluster.nodes[0].remotePort, 8087);
            assert.equal(myCluster.nodes[1].remoteAddress, '192.168.1.2');
            assert.equal(myCluster.nodes[1].remotePort, 8087);

            myCluster.execute(myCluster.nodes, {onError: function() {}});
            assert.equal(myCluster.nodeManager._nodeIndex, 1);
            myCluster.execute(myCluster.nodes, {onError: function() {}});
            assert.equal(myCluster.nodeManager._nodeIndex, 2);
            myCluster.execute(myCluster.nodes, {onError: function() {}});
            assert.equal(myCluster.nodeManager._nodeIndex, 1);
            done();
        });
        it('removing a node passing an existing RiakNode instance should return true', function (done) {
            var nodeAddrs = ['192.168.1.1:8087', '192.168.1.2:8087', '192.168.1.3:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddrs);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            myCluster.nodes.forEach(function (n) {
                n.state = RiakNode.State.RUNNING;
                n.execute = function() {
                    return true;
                };
            });
            var node = arrayOfNodes[0];
            assert(myCluster.removeNode(node));
            assert.equal(node.state, RiakNode.State.SHUTDOWN);
            assert.equal(myCluster.nodes.length, 2);
            assert.equal(myCluster.nodes[0].remoteAddress, '192.168.1.2');
            assert.equal(myCluster.nodes[0].remotePort, 8087);
            assert.equal(myCluster.nodes[1].remoteAddress, '192.168.1.3');
            assert.equal(myCluster.nodes[1].remotePort, 8087);
            done();
        });
        it ('removing a node passing an existing addr:[host] string should return true', function (done) {
            var nodeAddrs = ['192.168.1.1:8087', '192.168.1.2:8087', '192.168.1.3:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddrs);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            myCluster.nodes.forEach(function (n) {
                n.state = RiakNode.State.RUNNING;
                n.execute = function() {
                    return true;
                };
            });
            assert(myCluster.removeNode('192.168.1.2'));
            assert.equal(arrayOfNodes[1].state, RiakNode.State.SHUTDOWN);
            assert.equal(myCluster.nodes.length, 2);
            assert(myCluster.removeNode('192.168.1.1:8087'));
            assert.equal(arrayOfNodes[0].state, RiakNode.State.SHUTDOWN);
            assert.equal(myCluster.nodes.length, 1);
            assert.equal(myCluster.nodes[0].remoteAddress, '192.168.1.3');
            assert.equal(myCluster.nodes[0].remotePort, 8087);
            assert.equal(arrayOfNodes[2].state, RiakNode.State.RUNNING);
            done();
        });
        it ('removing a node passing a non-existing addr:[host] should return false', function (done) {
            var nodeAddrs = ['192.168.1.1:8087', '192.168.1.2:8087'];
            var arrayOfNodes = RiakNode.buildNodes(nodeAddrs);
            var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
            myCluster.nodes.forEach(function (n) {
                n.state = RiakNode.State.RUNNING;
                n.execute = function() {
                    return true;
                };
            });
            assert(!myCluster.removeNode('192.168.1.11'));
            assert.equal(myCluster.nodes.length, 2);
            assert(!myCluster.removeNode('192.168.1.1:1234'));
            assert.equal(myCluster.nodes.length, 2);
            assert.equal(myCluster.nodes[0].remoteAddress, '192.168.1.1');
            assert.equal(myCluster.nodes[0].remotePort, 8087);
            assert.equal(myCluster.nodes[0].state, RiakNode.State.RUNNING);
            assert.equal(myCluster.nodes[1].remoteAddress, '192.168.1.2');
            assert.equal(myCluster.nodes[1].remotePort, 8087);
            assert.equal(myCluster.nodes[1].state, RiakNode.State.RUNNING);
            done();
        });
    });
});
