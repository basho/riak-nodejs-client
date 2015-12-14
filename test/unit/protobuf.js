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

