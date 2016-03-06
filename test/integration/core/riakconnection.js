'use strict';

var assert = require('assert');
var fs = require('fs');
var net = require('net');

var Test = require('../testparams');
var RiakConnection = require('../../../lib/core/riakconnection');
var Ping = require('../../../lib/commands/ping');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

describe('integration-core-riakconnection', function() {
    describe('connect', function() {
        it('emits-on-success', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : port
            });
            var server = net.createServer(function(s) {});
            server.listen(port, '127.0.0.1', function () {
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
                remoteAddress : "127.0.0.1",
                remotePort : port,
            });
            conn.on('connectFailed', function() {
                conn.close();
                done();
            });
            conn.connect();
        });
       
        it('emits-on-closed-socket', function(done) {
            var port = Test.getPort();
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : port,
            });
            var server = net.createServer(function(socket) {
                socket.destroy();
            });
            server.listen(port, '127.0.0.1', function () {
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
                conn.close();
                done();
            });
            conn.connect();
        });
        
        it('emits-on-failed-healthcheck', function(done) {
            var port = Test.getPort();
            var hc =  new Ping(function(){});
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
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
            
            server.listen(port, '127.0.0.1', function () {
                conn.on('connectFailed', function() {
                    conn.close();
                    server.close(function () {
                        done();
                    });
                });
                conn.connect();
            });
        });
        
        it('emits-on-successful-healthcheck', function(done) {
            var conn = new RiakConnection({
                remoteAddress : "127.0.0.1",
                remotePort : 2341,
                healthCheck: new Ping(function(){})
            });

            var server = net.createServer(function(socket) {
                var header = new Buffer(5);
                header.writeUInt8(2, 4);
                header.writeInt32BE(1, 0);
                socket.write(header);
            });
            
            server.listen(2341, '127.0.0.1', function() {
                conn.on('connected', function() {
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
