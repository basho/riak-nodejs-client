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
var fs = require('fs');
var logger = require('winston');
var net = require('net');

var Test = require('../testparams');
var RiakConnection = require('../../../lib/core/riakconnection');
var Ping = require('../../../lib/commands/ping');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var testAddress = '127.0.0.1';

describe('integration-core-riakconnection', function() {
    describe('connect', function() {
        it('emits-on-successful-connect', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port
            });
            var server = net.createServer(function(s) {});
            server.listen(port, testAddress, function () {
                conn.on('connected', function() {
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('emits-on-fail', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port,
            });
            conn.on('connectFailed', function() {
                // NB: when connectFailed is raised, conn is already closed
                done();
            });
            conn.connect();
        });
       
        it('emits-on-closed-socket', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port,
            });
            var server = net.createServer(function(socket) {
                socket.destroy();
            });
            server.listen(port, testAddress, function () {
                conn.on('connectionClosed', function() {
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('emits-on-connect-timeout', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : "5.5.5.5",
                remotePort : port,
                connectionTimeout : 500
            });
            conn.on('connectFailed', function() {
                // NB: when connectFailed is raised, conn is already closed
                done();
            });
            conn.connect();
        });
        
        it('emits-on-failed-healthcheck', function(done) {
            var port = Test.getPort();

            var ping_success = true;
            var hc =  new Ping(function (err, rslt) {
                if (err || !rslt) {
                    ping_success = false;
                }
            });

            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port,
                healthCheck: hc
            });

            var server = net.createServer(function(socket) {
                var header = new Buffer(5);
                header.writeUInt8(0, 4);
                
                var rpbErr = new RpbErrorResp();
                rpbErr.setErrmsg(new Buffer('this is an error'));
                rpbErr.setErrcode(0);
                var encoded = rpbErr.encode().toBuffer();
                
                header.writeInt32BE(encoded.length + 1, 0);
                socket.write(header);
                socket.write(encoded);
            });
            
            server.listen(port, testAddress, function () {
                conn.on('connectFailed', function() {
                    // NB: when connectFailed is raised, conn is already closed
                    server.close(function () {
                        assert.strictEqual(ping_success, false);
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('emits-on-successful-healthcheck', function(done) {
            var ping_success = false;
            var ping = new Ping(function (err, rslt) {
                if (err || !rslt ) {
                    ping_success = false;
                } else {
                    ping_success = rslt;
                }
            });

            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port,
                healthCheck: ping
            });

            var server = net.createServer(function(socket) {
                var header = new Buffer(5);
                header.writeUInt8(2, 4);
                header.writeInt32BE(1, 0);
                socket.write(header);
            });
            
            server.listen(port, testAddress, function() {
                conn.on('connected', function() {
                    conn.close();
                    server.close(function () {
                        assert.strictEqual(ping_success, true);
                        done();
                    });
                });
                conn.connect();
            });
        });
    });

    describe('timeouts', function() {
        it('handles-read-timeout', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : testAddress,
                remotePort : port,
                requestTimeout: 100
            });

            var rt = null;
            var server = net.createServer(function(c) {
                logger.debug('[test/integration/core/riakconnection] client connected');
                c.on('end', function () {
                    clearTimeout(rt);
                    logger.debug('[test/integration/core/riakconnection] client disconnected');
                });
                c.on('error', function (err) {
                    clearTimeout(rt);
                    logger.debug('[test/integration/core/riakconnection] socket error:', err);
                });
                c.on('data', function (data) {
                    var cb = function (conn) {
                        logger.debug('[test/integration/core/riakconnection] sending ping response');
                        var header = new Buffer(5);
                        header.writeUInt8(2, 4);
                        header.writeInt32BE(1, 0);
                        conn.write(header);
                    };
                    rt = setTimeout(cb.bind(this, c), 250);
                });
            });
            
            var saw_closed = false;
            server.listen(port, testAddress, function() {
                // NB: callback won't be called, only RiakNode
                // does that
                var ping = new Ping(function () {});

                function cleanup(conn) {
                    conn.removeAllListeners();
                    conn.close();
                    assert.strictEqual(saw_closed, true);
                    server.close(function () {
                        done();
                    });
                }

                conn.on('connectionClosed', function(conn) {
                    saw_closed = true;
                    logger.debug('[test/integration/core/riakconnection] saw connectionClosed');
                    cleanup(conn);
                });

                conn.on('responseReceived', function(conn, cmd, msgCode, decoded) {
                    logger.debug('[test/integration/core/riakconnection] saw responseReceived');
                    assert.strictEqual(msgCode, cmd.getExpectedResponseCode());
                    cleanup(conn);
                });

                conn.on('connected', function() {
                    logger.debug('[test/integration/core/riakconnection] saw connected');
                    conn.execute(ping);
                });

                conn.connect();
            });
        });
    });
});
