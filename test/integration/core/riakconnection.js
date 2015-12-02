'use strict';

var assert = require('assert');
var fs = require('fs');
var net = require('net');

var Test = require('../testparams');
var RiakConnection = require('../../../lib/core/riakconnection');
var Ping = require('../../../lib/commands/ping');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

describe('RiakConnection - Integration', function() {

    describe('#connect', function() {
        
        this.timeout(5000);
        
        it('should emit on connection success', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : port,
                connectionTimeout : 30000
            });
            var server = net.createServer(function(socket) {
                    // no op
            });

            server.listen(port, '127.0.0.1', function () {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 1000); 
                conn.on('connected', function() {
                    clearTimeout(errTimeout);
                    conn.removeAllListeners();
                    assert(true);
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('should emit on connection fail', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({ remoteAddress : "127.0.0.1",
                                            remotePort : port,
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
            var port = Test.getPort();
            var conn = new RiakConnection({ remoteAddress : "127.0.0.1",
                                            remotePort : port,
                                            connectionTimeout : 30000
                                          });

            var server = net.createServer(function(socket) {
                socket.destroy();
            });

            server.listen(port, '127.0.0.1', function () {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 2000);
                conn.on('connectionClosed', function() {
                    clearTimeout(errTimeout);
                    conn.close();
                    server.close(function () {
                        assert(true);
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('should emit on connect timeout', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({ remoteAddress : "5.5.5.5",
                                            remotePort : port,
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
            var port = Test.getPort();

            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : port,
                connectionTimeout : 30000,
                healthCheck: new Ping(function(){})
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
            
            server.listen(port, '127.0.0.1', function () {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 30000); 
                
                conn.on('connectFailed', function() {
                    clearTimeout(errTimeout);
                    conn.removeAllListeners();
                    assert(true);
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('should emit on healthcheck success', function(done) {
           
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : 2341,
                connectionTimeout : 30000,
                healthCheck: new Ping(function(){})
            });

            var server = net.createServer(function(socket) {
                var header = new Buffer(5);
                header.writeUInt8(2, 4);
                header.writeInt32BE(1, 0);
                socket.write(header);
            });
            
            server.listen(2341, '127.0.0.1', function() {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 1000); 
                conn.on('connected', function() {
                    clearTimeout(errTimeout);
                    conn.removeAllListeners();
                    assert(true);
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
    });
});
