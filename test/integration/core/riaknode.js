'use strict';

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var net = require('net');

var Test = require('../testparams');
var RiakNode = require('../../../lib/core/riaknode');
var Ping = require('../../../lib/commands/ping');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');

describe('integration-core-riaknode', function() {
    describe('command-execution', function() {
        it('increments-execution-count', function(done) {
            var port = Test.getPort();
            var header = new Buffer(5);
            header.writeUInt8(2, 4);
            header.writeInt32BE(1, 0);

            var server = net.createServer(function(socket) {
                socket.on('data', function(data) {
                    socket.write(header);
                });
            });

            var lcb = function () {
                var node = new RiakNode.Builder()
                    .withRemotePort(port)
                    .withMinConnections(8)
                    .build();

                var scb = function (err, n) {
                    assert(Object.is(node, n));
                    var pingCount = 0;
                    var cb = function(err, resp) {
                        pingCount++;
                        assert(!err, err);
                        assert(resp, 'ping should return true!');
                        if (pingCount == 4) {
                            n.stop(function (err, rslt) {
                                assert(!err);
                                assert.equal(rslt, RiakNode.State.SHUTDOWN);
                                server.close(function () {
                                    done();
                                });
                            });
                        }
                    };
                    assert.equal(node.executeCount, 0);

                    assert(node.execute(new Ping(cb)));
                    assert.equal(node.executeCount, 1);

                    assert(node.execute(new Ping(cb)));
                    assert.equal(node.executeCount, 2);

                    assert(node.execute(new Ping(cb)));
                    assert.equal(node.executeCount, 3);

                    assert(node.execute(new Ping(cb)));
                    assert.equal(node.executeCount, 4);
                };

                node.start(scb);
            };
            
            server.listen({ host: '127.0.0.1', port: port }, lcb);
        });
    });

    describe('health-check', function() {
        it('recovers-using-default-check', function(done) {
            var socketClosed = false;
            var sawPing = false;
            var port = Test.getPort();
            var connects = 0;
            var server = net.createServer(function(socket) {
                connects++;
                if (connects === 1) {
                    socket.destroy();
                    socketClosed = true;
                } else {
                    socket.on('data' , function(data) {
                        // the ping got here
                        var header = new Buffer(5);
                        header.writeUInt8(2, 4);
                        header.writeInt32BE(1, 0);
                        socket.write(header);
                        sawPing = true;
                    });
                }
            });
            
            server.listen(port, '127.0.0.1', function () {
                var node = new RiakNode.Builder()
                        .withRemotePort(port)
                        .withMinConnections(0)
                        .withMaxConnections(1)
                        .build();
                var heathChecking = false;
                var healthChecked = false;
                var verifyCb = function(node, state) {
                    switch(state) {
                        case RiakNode.State.HEALTH_CHECKING:
                            heathChecking = true;
                            break;
                        case RiakNode.State.RUNNING:
                            healthChecked = true;
                            break;
                        default:
                            break;
                    }
                    if (heathChecking && healthChecked) {
                        node.removeAllListeners();
                        node.stop(function (err, rslt) {
                            assert(!err);
                            assert(socketClosed);
                            assert(sawPing);
                            assert.equal(rslt, RiakNode.State.SHUTDOWN);
                            server.close(function () {
                                done();
                            });
                        });
                    }
                };
                
                node.start(function (err, n) {
                    assert(Object.is(node, n));
                    assert(!err, err);
                    node.on('stateChange', verifyCb);
                    var fetchCb = function(err, resp) {
                        assert(err);
                    };
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
                    node.execute(fetch);
                });
            });
        });
        
        it('should recover using StoreValue as a check', function(done) {
            var port = Test.getPort();
            var connects = 0;
            var server = net.createServer(function(socket) {
                connects++;
                if (connects === 1) {
                    socket.destroy();
                } else {
                    socket.on('data' , function(data) {
                        // the StoreValue got here
                        var header = new Buffer(5);
                        header.writeUInt8(12, 4);
                        header.writeInt32BE(1, 0);
                        socket.write(header);
                    });
                }
            });
            
            server.listen(port, '127.0.0.1', function () {
                var storeCheck = new StoreValue({bucket: 'b', value: 'v'}, function(){});
                var node = new RiakNode.Builder()
                        .withRemotePort(port)
                        .withMinConnections(0)
                        .withHealthCheck(storeCheck)
                        .build();
                var heathChecking = false;
                var healthChecked = false;
                var verifyCb = function(node, state) {
                    switch(state) {
                        case RiakNode.State.HEALTH_CHECKING:
                            heathChecking = true;
                            break;
                        case RiakNode.State.RUNNING:
                            healthChecked = true;
                            break;
                        default:
                            break;
                    }
                    
                    if (heathChecking && healthChecked) {
                        node.removeAllListeners();
                        node.stop(function (err, rslt) {
                            assert(!err);
                            assert.equal(rslt, RiakNode.State.SHUTDOWN);
                            server.close(function () {
                                done();
                            });
                        });
                    }
                };
                
                node.start(function (err, n) {
                    assert(Object.is(node, n));
                    assert(!err, err);
                    node.on('stateChange', verifyCb);
                    var fetchCb = function(err, resp) {
                        assert(err);
                    };
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
                    node.execute(fetch);
                });
            });
        });
    });
});
