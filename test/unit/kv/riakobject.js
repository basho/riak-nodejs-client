'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var RiakObject = require('../../../lib/commands/kv/riakobject');
var RpbContent = rpb.getProtoFor('RpbContent');

var assert = require('assert');

describe('RiakObject', function() {
    it('should build correctly when convertToJs is true and isTombstone is true', function(done) {
        var value = 'this is a value';

        var rpbContent = new RpbContent();
        rpbContent.setValue(new Buffer(value));
        rpbContent.setDeleted(true);

        var ro = RiakObject.createFromRpbContent(rpbContent, true);
        assert(ro.isTombstone);
        assert(JSON.stringify(ro.value) === '{}');
        done();
    });
});
