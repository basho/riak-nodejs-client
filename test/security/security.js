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

var Test = require('../integration/testparams');
var RiakConnection = require('../../lib/core/riakconnection');
var assert = require('assert');
var fs = require('fs');

describe('RiakConnection - Integration', function() {

    describe('#connect-tls-clientcert', function() {
        this.timeout(10000);
        it('should emit on connection success', function(done) {

            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 30000,
                auth: {
                    // Use the following when the private key and public cert
                    // are in two file
                    // key: fs.readFileSync(''),
                    // cert: fs.readFileSync('')
                    user: 'riakuser',
                    // password: '', // NB: optional, leave null when using certs
                    pfx: fs.readFileSync('./tools/test-ca/certs/riakuser-client-cert.pfx'),
                    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ],
                    rejectUnauthorized: true
                }
            });

            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 5000); 

            conn.on('connected', function() {
                assert(true);
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                conn.close();
                done();
            });

            conn.connect();
        });
    });

    describe('#connect-tls-password', function() {
        this.timeout(10000);
        it('should emit on connection success', function(done) {

            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 5000,
                auth: {
                    user: 'riakpass',
                    password: 'Test1234',
                    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ],
                    rejectUnauthorized: true
                }
            });

            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 9500); 

            conn.on('connected', function() {
                assert(true);
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                conn.close();
                done();
            });

            conn.connect();
        });
    });

});

