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

var RiakNode = require('../../lib/core/riaknode');
var assert = require('assert');
var joi = require('joi');
var fs = require('fs');

describe('RiakNode', function() {
    describe('auth-validation', function() {

        it('should require user', function(done) {
            var options = {
                auth: {
                }
            };
            assert.throws(
                function() {
                    var rn = new RiakNode(options);
                },
                function (err) {
                    assert.equal(err.name, 'ValidationError');
                    return true;
                }
            );
            done();
        });

        it('should OK user with empty password specified', function(done) {
            // This shouldn't throw because it is allowed to have an empty password
            var options = {
                auth: {
                    user: 'riaktest',
                    password: ''
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should OK user and password', function(done) {
            // This shouldn't throw because user + password is a valid auth combo
            var options = {
                auth: {
                    user: 'riaktest',
                    password: 'Test234'
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should disallow user with just cert or just key', function(done) {
            // This should throw because *both* cert and key are required
            var options = {
                auth: {
                    user: 'riaktest',
                    cert: 'DUMMY CERT CONTENTS'
                }
            };
            assert.throws(
                function() {
                    var rn = new RiakNode(options);
                },
                function (err) {
                    assert.equal(err.name, 'ValidationError');
                    return true;
                }
            );
            options = {
                auth: {
                    user: 'riaktest',
                    key: 'DUMMY KEY CONTENTS'
                }
            };
            assert.throws(
                function() {
                    var rn = new RiakNode(options);
                },
                function (err) {
                    assert.equal(err.name, 'ValidationError');
                    return true;
                }
            );
            done();
        });

        it('should OK user with cert & key', function(done) {
            // This should not throw because *both* cert and key are provided
            var options = {
                auth: {
                    user: 'riaktest',
                    cert: 'DUMMY CERT CONTENTS',
                    key: 'DUMMY KEY CONTENTS'
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should OK user with cert & key read from a file', function(done) {
            // This should not throw because *both* cert and key are provided
            var options = {
                auth: {
                    user: 'riaktest',
                    cert: fs.readFileSync('./tools/test-ca/certs/riakuser-client-cert.pem'),
                    key: fs.readFileSync('./tools/test-ca/private/riakuser-client-cert-key.pem'),
                    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ],
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should OK user and pfx', function(done) {
            // This should not throw because pfx files contain both public and private keys
            var options = {
                auth: {
                    user: 'riaktest',
                    pfx: 'DUMMY PFX CONTENTS'
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should OK user and pfx read from file', function(done) {
            // This should not throw because pfx files contain both public and private keys
            var options = {
                auth: {
                    user: 'riaktest',
                    pfx: fs.readFileSync('./tools/test-ca/certs/riakuser-client-cert.pfx'),
                    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ]
                }
            };
            assert.doesNotThrow(
                function() {
                    var rn = new RiakNode(options);
                }
            );
            done();
        });

        it('should disallow password and pfx, or password and cert/key', function(done) {
            var options = {
                auth: {
                    user: 'riaktest',
                    password: 'Test1234',
                    pfx: 'DUMMY PFX CONTENTS'
                }
            };
            assert.throws(
                function() {
                    var rn = new RiakNode(options);
                },
                function (err) {
                    assert.equal(err.name, 'ValidationError');
                    return true;
                }
            );
            options = {
                auth: {
                    user: 'riaktest',
                    password: 'Test1234',
                    cert: 'DUMMY CERT CONTENTS',
                    key: 'DUMMY KEY CONTENTS'
                }
            };
            assert.throws(
                function() {
                    var rn = new RiakNode(options);
                },
                function (err) {
                    assert.equal(err.name, 'ValidationError');
                    return true;
                }
            );
            done();
        });
    });
});

