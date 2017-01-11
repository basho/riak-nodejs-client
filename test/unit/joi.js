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

var Joi = require('joi');
var assert = require('assert');

describe('Joi', function() {
    it('returns object being validated on success', function(done) {
        var cb = function (err, rslt) { };
        var schema = Joi.func().required();
        var self = this;
        Joi.validate(cb, schema, function(err, value) {
            if (err) {
                throw new Error('callback is required and must be a function.');
            }
            assert.strictEqual(value, cb);
            done();
        });
    });
});
