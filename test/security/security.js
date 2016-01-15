'use strict';

var Test = require('../integration/testparams');
var RiakConnection = require('../../lib/core/riakconnection');
var assert = require('assert');
var fs = require('fs');
var logger = require('winston');

describe('RiakConnection - Integration', function() {
    this.timeout(10000);
    describe('connect-tls-clientcert', function() {
        it('should emit on connection success', function(done) {
            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 500,
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
            }, 1000); 

            function cleanup() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                conn.close();
                done();
            }

            conn.on('connectFailed', function (c, err) {
                assert(!err, err);
                cleanup();
            });

            conn.on('connected', function (c) {
                cleanup();
            });

            conn.connect();
        });
    });

    describe('connect-tls-password', function() {
        it('should emit on connection success', function(done) {
            var conn = new RiakConnection({
                remoteAddress : Test.riakHost,
                remotePort : Test.riakPort,
                connectionTimeout : 500,
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
            }, 1000); 

            function cleanup() {
                clearTimeout(errTimeout);
                conn.removeAllListeners();
                conn.close();
                done();
            }

            conn.on('connectFailed', function (c, err) {
                assert(!err, err);
                cleanup();
            });

            conn.on('connected', function() {
                cleanup();
            });

            conn.connect();
        });
    });
});
