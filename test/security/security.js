'use strict';

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

