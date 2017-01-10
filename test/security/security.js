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
var logger = require('winston');

var Ping = require('../../lib/commands/ping');
var Test = require('../integration/testparams');
var RiakConnection = require('../../lib/core/riakconnection');

function cleanup(conn, done) {
    conn.removeAllListeners();
    conn.close();
    done();
}

function ping(conn, done) {
    var p = new Ping(function () {});
    conn.on('responseReceived', function (c, cmd, mc, pb) {
        assert.strictEqual(mc, cmd.getExpectedResponseCode());
        cleanup(c, done);
    });
    conn.execute(p);
}

function setup_events(conn, done) {
    conn.on('connectFailed', function (c, err) {
        assert(!err, err);
        cleanup(c, done);
    });
    conn.on('connected', function (c) {
        ping(c, done);
    });
}

describe('security', function() {
    describe('connect-tls-clientcert', function() {
        it('emits-connected-then-ping', function(done) {
            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
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
                auth: Test.passAuth
            });
            setup_events(conn, done);
            conn.connect();
        });
    });
});
