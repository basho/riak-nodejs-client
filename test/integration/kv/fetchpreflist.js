'use strict';

var Test = require('../testparams');
var FetchPreflist = require('../../../lib/commands/kv/fetchpreflist');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');
var logger = require('winston');

var cb = function (err, resp) {
    assert(!err, err);
    assert.equal(resp.preflist.length, 3); // NB: since nval is 3
};

describe('FetchPreflist - Integration', function() {
    var cluster;
    this.timeout(10000);

    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });

    it('Should fetch a preflist from Riak (default type)', function(done) {
        var fetch = new FetchPreflist.Builder()
                .withBucket(Test.bucketName)
                .withKey('my_key1')
                .withCallback(function (err, resp) { cb(err, resp); done(); })
                .build();
        
        cluster.execute(fetch);
    });
    
    it('Should fetch a preflist for a non-default bucket-type from Riak', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.preflist.length, 3); // NB: since nval is 3
            done();
        };
        
        var fetch = new FetchPreflist.Builder()
                .withBucket(Test.bucketName)
                .withBucketType(Test.bucketType)
                .withKey('my_key1')
                .withCallback(function (err, resp) { cb(err, resp); done(); })
                .build();
        
        cluster.execute(fetch);
    });
});
