/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

        it('does-not-crash-when-connection-closes', function(done) {
            var port = Test.getPort();
            var destroyed = false;
            var server = net.createServer(function(socket) {
                socket.destroy();
                destroyed = true;
            });
            var lcb = function () {
                var node = new RiakNode.Builder()
                    .withRemotePort(port)
                    .withMinConnections(1)
                    .build();
                var scb = function (err, n) {
                    function endTest() {
                        if (destroyed) {
                            n.stop(function (err, rslt) {
                                assert(!err);
                                assert.equal(rslt, RiakNode.State.SHUTDOWN);
                                server.close(function () {
                                    done();
                                });
                            });
                        } else {
                            setTimeout(endTest, 10);
                        }
                    }
                    setTimeout(endTest, 10);
                };
                node.start(scb);
            };
            server.listen({ host: '127.0.0.1', port: port }, lcb);
        });
    });

    describe('load-balancer', function() {
        it('does-not-health-check', function(done) {
            var port = Test.getPort();
            var i = 0;
            var server = net.createServer(function(socket) {
                socket.on('data' , function(data) {
                    i++;
                    logger.debug('[t/i/c/rn] i: %d', i);
                    if (i % 2 === 0) {
                        socket.destroy();
                    } else {
                        var header = new Buffer(5);
                        header.writeUInt8(2, 4);
                        header.writeInt32BE(1, 0);
                        socket.write(header);
                    }
                });
            });

            server.listen(port, '127.0.0.1', function () {
                var node = new RiakNode.Builder()
                        .withRemotePort(port)
                        .withMinConnections(1)
                        .withMaxConnections(1)
                        .withExternalLoadBalancer(true)
                        .build();

                var sawHealthCheck = false;

                function ping() {
                    logger.debug('[t/i/c/rn] executing ping, i: %d', i);
                    var ping = new Ping(pingCb);
                    node.execute(ping);
                }

                var pingCb = function(err, resp) {
                    if (err) {
                        logger.debug('[t/i/c/rn] ping err:', err);
                    }
                    if (i < 10) {
                        setTimeout(ping, 125);
                    } else {
                        node.removeAllListeners();
                        node.stop(function (err, rslt) {
                            assert(!err);
                            assert.strictEqual(sawHealthCheck, false);
                            assert.equal(rslt, RiakNode.State.SHUTDOWN);
                            server.close(function () {
                                done();
                            });
                        });
                    }
                };

                var stateCb = function(node, state) {
                    if (state === RiakNode.State.HEALTH_CHECKING) {
                        sawHealthCheck = true;
                    }
                };

                node.start(function (err, n) {
                    assert(Object.is(node, n));
                    assert(!err, err);
                    node.on('stateChange', stateCb);
                    ping();
                });
            });
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
