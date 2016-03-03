'use strict';

var assert = require('assert');
var logger = require('winston');
var net = require('net');

var Test = require('./testparams');
var Riak = require('../../index.js');

var host = '127.0.0.1';
var header = new Buffer(5);
header.writeUInt8(2, 4);
header.writeInt32BE(1, 0);

function startServer(cb) {
    var server = net.createServer(function(socket) {
        socket.on('data', function(data) {
            socket.write(header);
        });
    });

    var port = Test.getPort();
    var sopts = { host: host, port: port };
    server.listen(sopts, function() {
        logger.debug('listener started');
        cb(port, server);
    });
}

describe('integration-client', function() {
    it('array-of-addrs-without-callback', function(done) {
        startServer(function (port, server) {
            var addr = host + ':' + port;
            var c = new Riak.Client([addr]);
            setTimeout(function () {
                c.shutdown(function (state) {
                    if (state === Riak.Cluster.State.SHUTDOWN) {
                        server.close(function () {
                            done();
                        });
                    }
                });
            }, 250);
        });
    });

    it('array-of-addrs-with-callback', function(done) {
        startServer(function (port, server) {
            var addr = host + ':' + port;
            var c = new Riak.Client([addr, addr], function (err, client) {
                assert(Object.is(c, client));
                c.stop(function (err, rslt) {
                    assert(!err, err);
                    server.close(function () {
                        done();
                    });
                });
            });
        });
    });

    it('RiakCluster-without-callback', function(done) {
        startServer(function (port, server) {
            var nopts = {
                remoteAddress: host,
                remotePort: port
            };
            var node = new Riak.Node(nopts);
            var copts = {
                nodes: [ node ]
            };
            var cl = new Riak.Cluster(copts);
            var c = new Riak.Client(cl);
            setTimeout(function () {
                c.shutdown(function (state) {
                    if (state === Riak.Cluster.State.SHUTDOWN) {
                        server.close(function () {
                            done();
                        });
                    }
                });
            }, 250);
        });
    });

    it('RiakCluster-with-callback', function(done) {
        startServer(function (port, server) {
            var nopts = {
                remoteAddress: host,
                remotePort: port
            };
            var node = new Riak.Node(nopts);
            var copts = {
                nodes: [ node ]
            };
            var cl = new Riak.Cluster(copts);
            var c = new Riak.Client(cl, function (err, client) {
                assert(Object.is(c, client));
                assert(!err, err);
                client.stop(function (err, rslt) {
                    assert(!err);
                    assert.equal(rslt, Riak.Cluster.State.SHUTDOWN);
                    server.close(function () {
                        done();
                    });
                });
            });
        });
    });
});
