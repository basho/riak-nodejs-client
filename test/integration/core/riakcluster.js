'use strict';

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var net = require('net');

var Test = require('../testparams');
var RiakCluster = require('../../../lib/core/riakcluster');
var RiakNode = require('../../../lib/core/riaknode');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var Ping = require('../../../lib/commands/ping');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');

describe('integration-core-riakcluster', function() {
    describe('node-selection', function() {
        it('tries-all-nodes-with-default-nodemanager', function(done) {
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
                            var onClose = function (err) {
                                closed++;
                                if (err) {
                                    logger.error('server.close error: ', err);
                                }
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
            for (i = 0; i < nodeCount; i++) {
                var port = Test.getPort();
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
                cluster.start(function (err, c) {
                    assert(Object.is(cluster, c));
                    assert(!err, err);
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, function(){});
                    cluster.execute(fetch);
                });
            });
        });
    });
    
    describe('command-queuing', function() {
        this.timeout(4000); // NB: needed for Windows
        it('queues-commands-and-re-tries', function(done) {
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
                cluster.stop(function (err, state) {
                    assert(!err);
                    assert.equal(state, RiakCluster.State.SHUTDOWN);
                    server.close(function (err) {
                        if (err) {
                            logger.error('server.close error: ', err);
                        }
                        done();
                    });
                });
            };
            
            cluster.on('stateChange', stateMe);
            cluster.start(function (err, c) {
                assert(Object.is(cluster, c));
                assert(err);
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
            });
        });
        
        it('no-queuing-by-default', function(done) {
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

            cluster.start(function (err, c) {
                assert(Object.is(cluster, c));
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
            });
        });
        
        it ('no-more-queuing-if-maxDepth-reached', function(done) {
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

            cluster.start(function (err, c) {
                assert(Object.is(cluster, c));
                var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
                cluster.execute(store);
                cluster.execute(store);
            });
        });
    });

    describe('connection-tests', function() {
        var nc = 3;
        var ping_count = 9;

        function writePingResp(socket) {
            var rsp = new Buffer(5);
            rsp.writeUInt8(2, 4);
            rsp.writeInt32BE(1, 0);
            socket.write(rsp);
        }

        function runTest(onDataFunc, done, clusterBldrCb, nodeBldrCb) {
            function makeServerListenFunc(server, port) {
                var f = function(async_cb) {
                    server.listen(port, '127.0.0.1', function() {
                        logger.debug('listening on port: ', port);
                        async_cb();
                    });
                };
                return f;
            }

            function makeCreateServerCallback(onDataFunc) {
                var datas = 0;
                return function(socket) {
                    logger.debug('[riakcluster/connection-tests] client connected');
                    socket.on('data', function(data) {
                        datas = onDataFunc(socket, datas);
                    });
                    socket.on('end', function () {
                        logger.debug('[riakcluster/connection-tests] client disconnected');
                    });
                    socket.on('error', function (err) {
                        logger.debug('[riakconnection/connection-tests] socket error:', err);
                    });
                };
            }

            var ping_successes = 0;
            function makePingFunc(i, c) {
                var f = function(async_cb) {
                    var p = new Ping(function (err, rslt) {
                        if (err) {
                            logger.debug('ping err: ', err);
                        }
                        if (rslt === true) {
                            ping_successes++;
                            logger.debug('%d ping success! ping successes: %d', i, ping_successes);
                        }
                        async_cb(null, rslt);
                    });
                    c.execute(p);
                };
                return f;
            }

            var ports = [];
            var servers = [];
            var serverListenFuncs = [];
            for (var i = 0; i < nc; i++) {
                var port = Test.getPort();
                ports.push(port);

                var cs_cb = makeCreateServerCallback(onDataFunc);
                var server = net.createServer(cs_cb);
                servers.push(server);

                var sl_cb = makeServerListenFunc(server, port);
                serverListenFuncs.push(sl_cb);
            }

            async.parallel(serverListenFuncs, function (err, rslts) {
                assert(!err, err);
                var nodes = [];
                ports.forEach(function (p) {
                    var node_bldr = new RiakNode.Builder()
                        .withRemotePort(p);
                    if (nodeBldrCb) {
                        nodeBldrCb(node_bldr);
                    }
                    nodes.push(node_bldr.build());
                });
                var cluster_bldr = new RiakCluster.Builder()
                    .withRiakNodes(nodes);
                if (clusterBldrCb) {
                    clusterBldrCb(cluster_bldr);
                }
                var cluster = cluster_bldr.build();
                cluster.start(function (err, c) {
                    assert(Object.is(cluster, c));
                    assert(!err, err);
                    var funcs = [];
                    for (var i = 0; i < ping_count; i++) {
                        funcs.push(makePingFunc(i, cluster));
                    }
                    async.parallel(funcs, function (err, rslts) {
                        if (err) {
                            logger.error('async_cb error: ', err);
                        }
                        var wait_count = 0;
                        var clusterStopFunc = function() {
                            logger.debug('ping successes: %d / ping_count: %d ', ping_successes, ping_count);
                            if (ping_successes < ping_count && wait_count < 20) {
                                logger.debug('waiting on ping successes: ', ping_successes);
                                wait_count++;
                                setTimeout(clusterStopFunc, 100);
                                return;
                            }
                            assert.equal(ping_successes, ping_count);
                            cluster.stop(function (err, rslt) {
                                if (err) {
                                    logger.error('cluster.stop error: ', err);
                                }
                                logger.debug('closing %d servers', servers.length);
                                var closeFuncs = [];
                                servers.forEach(function (server) {
                                    closeFuncs.push(function (acb) {
                                        server.close(function (err) {
                                            if (err) {
                                                logger.error('server.close error: ', err);
                                            }
                                            acb();
                                        });
                                    });
                                });
                                async.parallel(closeFuncs, function (err, rslts) {
                                    assert(!err, err);
                                    done();
                                });
                            });
                        };
                        clusterStopFunc();
                    });
                });
            });
        }

        it('handles-closed-connections', function(done) {
            function onDataFunc(socket, datas) {
                datas++;
                logger.debug('datas: ', datas);
                if (datas % 3 === 0) {
                    socket.destroy();
                } else {
                    writePingResp(socket);
                }
                return datas;
            }

            runTest(onDataFunc, done);
        });

        it('handles-read-timeouts', function(done) {
            function onDataFunc(socket, datas) {
                datas++;
                if (datas % 2 === 0) {
                    logger.debug('writing delayed ping response:', datas);
                    setTimeout(function () {
                        writePingResp(socket);
                    }, 250);
                } else {
                    logger.debug('writing immediate ping response:', datas);
                    writePingResp(socket);
                }
                return datas;
            }

            runTest(onDataFunc, done,
                function (cluster_bldr) {
                    cluster_bldr.withExecutionAttempts(8);
                },
                function (node_bldr) {
                    node_bldr.withRequestTimeout(125);
                });
        });
    });
});
