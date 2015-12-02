'use strict';

var Riak = require('../lib/client');

var logger = require('winston');
var rs = require('randomstring');

var bucketType = 'no_siblings';
var bucket = 'kv_benchmarks';

var strings = [];
var i = 0;
for (i = 0; i < 32; ++i) {
    strings[i] = rs.generate((i + 1) * 1024);
}

var corkNodes = [];
var noCorkNodes = [];
for (var i = 17; i <= 47; i += 10) {
    var port = 10000 + i;
    corkNodes.push(new Riak.Node({ remoteAddress: 'riak-test', remotePort: port.toString(), cork: true }));
    noCorkNodes.push(new Riak.Node({ remoteAddress: 'riak-test', remotePort: port.toString(), cork: false }));
}

var corkCluster = new Riak.Cluster.Builder().withRiakNodes(corkNodes).build();
var corkClient = new Riak.Client(corkCluster);

var noCorkCluster = new Riak.Cluster.Builder().withRiakNodes(noCorkNodes).build();
var noCorkClient = new Riak.Client(noCorkCluster);

i = 0;

function getContent() {

    ++i;
    if (i >= 32 ) {
        i = 0;
    }

    var string = strings[i];

    var robj = new Riak.Commands.KV.RiakObject()
        .setBucketType(bucketType)
        .setBucket(bucket)
        .setKey(i.toString())
        .setContentType('text/plain')
        .setValue(string);

    return robj;
}

function kv(deferred, useCork) {

    var client = useCork ? corkClient : noCorkClient;

    var f_callback = function(err, resp) {
        if (err) {
            logger.error("[benchmarks/kv] %s", err);
            throw new Error(err);
        } else {
            var robj = resp.values.shift();
            var length = robj.getValue().toString('utf8').length;
            logger.debug("[benchmarks/kv] %d value length: %d", i, length);
        }
        deferred.resolve();
    };

    var s_callback = function(err, resp) {
        if (err) {
            logger.error("[benchmarks/kv] %s", err);
            throw new Error(err);
        } else {
            var fetch = new Riak.Commands.KV.FetchValue.Builder()
                .withBucketType(bucketType)
                .withBucket(bucket)
                .withKey(i.toString())
                .withCallback(f_callback)
                .build();

            client.execute(fetch);
        }
    };

    var robj = getContent();

    logger.debug("[benchmarks/kv] storing: %s", JSON.stringify(robj));

    var store = new Riak.Commands.KV.StoreValue.Builder()
            .withContent(robj)
            .withCallback(s_callback)
            .build();

    client.execute(store);
}

module.exports = {
    name: 'KV Benchmarks',
    tests: [
        {
            name: 'KV with cork()',
            defer: true,
            fn: function (deferred) {
                kv(deferred, true);
            }
        },
        {
            name: 'KV without cork()',
            defer: true,
            fn: function (deferred) {
                kv(deferred, false);
            }
        }
    ]
};
