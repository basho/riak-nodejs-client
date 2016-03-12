'use strict';

var assert = require('assert');

var Test = require('../testparams');
var StoreIndex = require('../../../lib/commands/yokozuna/storeindex');
var FetchIndex = require('../../../lib/commands/yokozuna/fetchindex');
var DeleteIndex = require('../../../lib/commands/yokozuna/deleteindex');

describe('Update and Fetch Yokozuna index - Integration', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp);
                done();
            };
            var store = new StoreIndex.Builder()
                    .withIndexName('myIndex')
                    .withCallback(callback)
                    .build();
            cluster.execute(store);
        });
    });
    
    after(function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            cluster.stop(function (err, rslt) {
                assert(!err, err);
                done();
            });
        };
        var del = new DeleteIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();
        cluster.execute(del);
    });
    
    it('Should fetch an index', function(done) {
        var count = 0;
        var callback = function(err, resp) {
            count++;
            if(err && err === 'notfound') {
                if (count < 6) {
                    setTimeout(fetchme, 2000 * count);
                } else {
                    assert(!err, err);
                }
            } else {
                assert(resp);
                done();
            }
        };

        var fetchme = function() {
            var fetch = new FetchIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();
            cluster.execute(fetch);
        };
        fetchme();
    });
});
