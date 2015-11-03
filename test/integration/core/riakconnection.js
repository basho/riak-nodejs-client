'use strict';

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

var Test = require('../testparams');
var RiakConnection = require('../../../lib/core/riakconnection');
var assert = require('assert');
var fs = require('fs');
var Ping = require('../../../lib/commands/ping');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

describe('RiakConnection - Integration', function() {

    describe('#connect', function() {
        
        this.timeout(5000);
        
        it('should emit on connection success', function(done) {
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : 1337,
                connectionTimeout : 30000
            });
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 1000); 
            
            var net = require('net');
 
            var server = net.createServer(function(socket) {
                    // no op
            });

            server.listen(1337, '127.0.0.1');
            
            conn.on('connected', function() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                assert(true);
                server.close();
                conn.close();
                done();
            });
            
            conn.connect();

        });
        
        it('should emit on connection fail', function(done) {
            var conn = new RiakConnection({ remoteAddress : "127.0.0.1",
                                            remotePort : 65535, // NB: using 0 causes an exception
                                            connectionTimeout : 1000
                                          });
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 30000); 
            
            conn.on('connectFailed', function() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                assert(true);
                done();
            });
            
            conn.connect();
            
        });
       
        it('should emit on socket closing', function(done) {
            var conn = new RiakConnection({ remoteAddress : "127.0.0.1",
                                            remotePort : 1337,
                                            connectionTimeout : 30000
                                          });
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 2000);
            
            var net = require('net');
 
            var server = net.createServer(function(socket) {
                    socket.destroy();
            });

            server.listen(1337, '127.0.0.1');
            
            conn.on('connectionClosed', function() {
                clearTimeout(errTimeout);
                conn.close();
                server.close();
                assert(true);
                done();
            });
            
            conn.connect();
            
        });
        
        it('should emit on connect timeout', function(done) {
            var conn = new RiakConnection({ remoteAddress : "5.5.5.5",
                                            remotePort : 1337,
                                            connectionTimeout : 1500
                                          });
                                          
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 4000); 
            
            conn.on('connectFailed', function() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                assert(true);
                done();
            });
            
            conn.connect();
            
        });
        
        it('should emit on healthcheck fail', function(done) {
           
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : 1337,
                connectionTimeout : 30000,
                healthCheck: new Ping(function(){})
            });
            
            var net = require('net');
           
            
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
            
            server.listen(1337, '127.0.0.1');
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 30000); 
            
            conn.on('connectFailed', function() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                assert(true);
                server.close();
                conn.close();
                done();
            });
            
            conn.connect();
            
        });
        
        it('should emit on healthcheck success', function(done) {
           
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : 1337,
                connectionTimeout : 30000,
                healthCheck: new Ping(function(){})
            });
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 1000); 
            
            var net = require('net');
           
            
            var server = net.createServer(function(socket) {
                    var header = new Buffer(5);
                    header.writeUInt8(2, 4);
                    header.writeInt32BE(1, 0);
                    socket.write(header);
            });
            
            server.listen(1337, '127.0.0.1');
            
            conn.on('connected', function() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                assert(true);
                server.close();
                conn.close();
                done();
            });
            
            conn.connect();
            
        });
    });
});
