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

var assert = require('assert');
var AuthReq = require('../../../lib/core/authreq');

describe('AuthReq', function() {
    it('sets user and password from options', function(done) {
        var user = 'user';
        var password = 'password';
        var opts = {
            user: user,
            password: password
        };
        var cb = function (e, r) { };
        var r = new AuthReq(opts, cb);
        assert.strictEqual(r.user, user);
        assert.strictEqual(r.password, password);
        done();
    });
});
