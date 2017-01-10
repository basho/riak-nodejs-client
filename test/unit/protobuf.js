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

var rpb = require('../../lib/protobuf/riakprotobuf');
var TsCell = rpb.getProtoFor('TsCell');

var assert = require('assert');

describe('Protobuf', function() {
    it('uses null to mean "value not set"', function(done) {
        var tsc = new TsCell();
        assert.strictEqual(tsc.getVarcharValue(), null);
        assert.strictEqual(tsc.getSint64Value(), null);
        assert.strictEqual(tsc.getTimestampValue(), null);
        assert.strictEqual(tsc.getBooleanValue(), null);
        assert.strictEqual(tsc.getDoubleValue(), null);
        done();
    });
});

