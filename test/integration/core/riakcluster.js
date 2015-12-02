'use strict';

var assert = require('assert');
var async = require('async');
var net = require('net');

var Test = require('../testparams');
var RiakCluster = require('../../../lib/core/riakcluster');
var RiakNode = require('../../../lib/core/riaknode');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');

describe('RiakCluster - Integration', function() {
   
    describe('Node selection', function() {
       
        it('should try all three nodes with the default NodeManager', function(done) {
            var port = Test.getPort();
            var nodeCount = 3;
            var servers = new Array(nodeCount);
            var nodes = new Array(nodeCount);
            var i = 0;
            var tried = 0;
            var errTimeout;
            var cluster;
            
            var header = new Buffer(5);
            header.writeUInt8(0, 4);

            var err = new RpbErrorResp();
            err.setErrmsg(new Buffer('Some error'));
            err.setErrcode(0);
            var encoded = err.encode().toBuffer();
            header.writeInt32BE(encoded.length + 1, 0);
            
            var sockMe = function(socket) {
                socket.on('data' , function(data) {
                    // the fetch got here
                    socket.write(header);
                    socket.write(encoded);
                    tried++;
                    if (tried === nodeCount) {
                        clearTimeout(errTimeout);
                        cluster.stop(function (err, rslt) {
                            assert(!err);
                            assert.equal(rslt, RiakCluster.State.SHUTDOWN);
                            var closed = 0;
                            var onClose = function () {
                                closed++;
                                if (closed === nodeCount) {
                                    done();
                                }
                            };
                            servers.forEach(function (s) {
                                s.close(onClose);
                            });
                        });
                    }
                });
            };
            
            var makeListenFunc = function(s, port) {
                var f = function(async_cb) {
                    s.listen(port, '127.0.0.1', async_cb);
                };
                return f;
            };

            var funcs = [];
            for (i = 0; i < nodeCount; i++, port++) {
                servers[i] = net.createServer(sockMe);
                funcs.push(makeListenFunc(servers[i], port));
                nodes[i] = new RiakNode.Builder()
                    .withRemotePort(port)
                    .withMinConnections(0)
                    .build();
            }
            async.parallel(funcs, function (err, rslts) {
                errTimeout = setTimeout(function () {
                    assert(false, 'All nodes weren\'t tried');
                    done();
                }, 3000); 
                
                cluster = new RiakCluster({nodes: nodes});
                cluster.start(function (err, rslt) {
                    assert(!err, err);
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, function(){});
                    cluster.execute(fetch);
                });
            });
        });
    });
    
    describe('Command queueing', function() {
        this.timeout(5000);

        it('should queue commands and retry from the queue, respecting queueSubmitInterval', function(done) {
            var port = Test.getPort();

            var server = net.createServer(function(socket) {
                socket.on('data', function(data) {
                    // it got here
                    var header = new Buffer(5);
                    header.writeUInt8(12, 4);
                    header.writeInt32BE(1, 0);
                    socket.write(header);
                });
            });
           
            var storeCheck = new StoreValue({bucket: 'b', value: 'v'}, function(){});
            var node = new RiakNode.Builder()
                .withRemotePort(port)
                .withHealthCheck(storeCheck)
                .build();         
            var cluster = new RiakCluster.Builder()
                .withRiakNodes([node])
                .withQueueCommands(undefined, 600)
                .build();

            var queueStart;

            var stateMe = function(state) {
                assert.equal(typeof(state), 'number', 'stateType');
                if (state === RiakCluster.State.QUEUEING) {
                    queueStart = Date.now();
                    server.listen(port, '127.0.0.1');
                }
            };
            
            var callMe = function(err, resp) {
                assert(!err, err);
                assert(Date.now() - queueStart >= 600, 'queueSubmitInterval respected');
                cluster.stop(function (err, rslt) {
                    assert(!err);
                    assert.equal(rslt, RiakCluster.State.SHUTDOWN);
                    done();
                });
            };
            
            cluster.on('stateChange', stateMe);
            cluster.start(function (err, rslt) {
                assert(err);
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
            });
        });
        
        it('should not queue by default', function(done) {
            var port = Test.getPort();

            var node = new RiakNode.Builder()
                   .withRemotePort(port)
                   .build();         
            var cluster = new RiakCluster.Builder()
                   .withRiakNodes([node])
                   .build();

            var callMe = function(err, resp) {
                assert(err);
                cluster.stop(function (err, rslt) {
                    assert(!err);
                    assert.equal(rslt, RiakCluster.State.SHUTDOWN);
                    done();
                });
            };

            cluster.start(function (err, rslt) {
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
            });
        });
        
        it ('should not queue if maxDepth is reached', function(done) {
            var port = Test.getPort();
           
            var node = new RiakNode.Builder()
                   .withRemotePort(port)
                   .build();         
            var cluster = new RiakCluster.Builder()
                   .withRiakNodes([node])
                   .withQueueCommands(1)
                   .build();

            var callMe = function(err, resp) {
                assert(err);
                cluster.stop(function (err, rslt) {
                    assert(!err);
                    assert.equal(rslt, RiakCluster.State.SHUTDOWN);
                    done();
                });
            };

            cluster.start(function (err, rslts) {
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
                cluster.execute(store);
            });
        });
    });
});
