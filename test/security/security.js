'use strict';

var assert = require('assert');
var logger = require('winston');

var Ping = require('../../lib/commands/ping');
var Test = require('../integration/testparams');
var RiakConnection = require('../../lib/core/riakconnection');

function ping_cb(err, rslt) {
    assert(!err, err);
    assert.strictEqual(rslt, true);
}

function ping(conn) {
    var p = new Ping(ping_cb);
    conn.execute(p);
}

function cleanup(conn, done) {
    conn.removeAllListeners();
    conn.close();
    done();
}

function setup_events(conn, done) {
    conn.on('connectFailed', function (c, err) {
        assert(!err, err);
        cleanup(c, done);
    });

    conn.on('responseReceived', function (c, cmd, msgCode) {
        assert.strictEqual(msgCode, cmd.expectedCode);
        cleanup(c, done);
    });

    conn.on('connected', function (c) {
        ping(c);
    });
}

describe('security', function() {
    describe('connect-tls-clientcert', function() {
        it('emits-connected-then-ping', function(done) {
            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 500,
                auth: Test.certAuth
            });
            setup_events(conn, done);
            conn.connect();
        });
    });

    describe('connect-tls-password', function() {
        it('emits-connected-then-ping', function(done) {
            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 500,
                auth: Test.passAuth
            });
            setup_events(conn, done);
            conn.connect();
        });
    });
});
