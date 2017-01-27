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

var assert = require('assert');
var fs = require('fs');
var logger = require('winston');

var Node = require('../../lib/core/riaknode');
var Cluster = require('../../lib/core/riakcluster');
var DeleteValue = require('../../lib/commands/kv/deletevalue');
var ListKeys = require('../../lib/commands/kv/listkeys');

var port = 2999;
module.exports.getPort = function () {
    port++;
    return port;
};

module.exports.bucketName = 'riak-nodejs-client';

/**
* Bucket type
*
* you must create the type 'leveldb_type' to use this:
*
* riak-admin bucket-type create leveldb_type '{"props":{"backend":"leveldb_backend"}}'
* riak-admin bucket-type activate leveldb_type
*/
module.exports.bucketType = 'plain';

var riakHost = 'localhost';
var riakPort = 8087;
if (process.env.RIAK_HOST) {
    riakHost = String(process.env.RIAK_HOST);
}
if (process.env.RIAK_PORT) {
    riakPort = Number(process.env.RIAK_PORT);
}
var nodeAddresses = [ riakHost + ':' + riakPort ];

var certAuth = {
    // Use the following when the private key and public cert
    // are in two file
    // key: fs.readFileSync(''),
    // cert: fs.readFileSync('')
    user: 'riakuser',
    // password: '', // NB: optional, leave null when using certs
    pfx: fs.readFileSync('./tools/test-ca/certs/riakuser-client-cert.pfx'),
    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ],
    rejectUnauthorized: true
};

var passAuth = {
    user: 'riakpass',
    password: 'Test1234',
    ca: [ fs.readFileSync('./tools/test-ca/certs/cacert.pem') ],
    rejectUnauthorized: true
};

module.exports.nodeAddresses = nodeAddresses;
module.exports.riakHost = riakHost;
module.exports.riakPort = riakPort;
module.exports.certAuth = certAuth;
module.exports.passAuth = passAuth;

module.exports.buildCluster = function(start_cb) {
    var nb = new Node.Builder()
        .withRemoteAddress(riakHost)
        .withRemotePort(riakPort);
    if (process.env.RIAK_NODEJS_CLIENT_SECURITY) {
        nb.withAuth(certAuth);
    }
    var cluster = new Cluster({ nodes: [ nb.build() ]});
    cluster.start(start_cb);
    return cluster;
};

/**
 *
 * CRDTs - need to create these types
 */
module.exports.counterBucketType = 'counters';
module.exports.setBucketType = 'sets';
module.exports.gsetBucketType = 'gsets';
module.exports.mapBucketType = 'maps';
module.exports.hllBucketType = 'hlls';

module.exports.cleanBucket = function(cluster, type, bucket, callback) {
    // Note this also acts as the integration test for ListKeys and
    // DeleteValue
    logger.debug('Clearing bucket: %s:%s', type, bucket);
    var numKeys = 0;
    var count = 0;
    var lkCallback = function(err, resp) {
        assert(!err, err);

        numKeys += resp.keys.length;

        if (numKeys > 0) {
            logger.debug('\tkey count to delete %s', numKeys);
        } else {
            logger.debug('DONE clearing bucket: %s:%s', type, bucket);
            callback();
        }

        var dCallback = function(err, resp) {
            assert(!err, err);
            count++;
            if (count === numKeys) {
                logger.debug('DONE clearing bucket: %s:%s', type, bucket);
                callback();
            }
        };

        for (var i = 0; i < resp.keys.length; i++) {
            var del = new DeleteValue.Builder()
                    .withBucket(resp.bucket)
                    .withBucketType(resp.bucketType)
                    .withKey(resp.keys[i])
                    .withCallback(dCallback)
                    .build();
            cluster.execute(del);
        }
    };

    var list = new ListKeys.Builder()
            .withAllowListing()
            .withBucket(bucket)
            .withBucketType(type)
            .withCallback(lkCallback)
            .withStreaming(false)
            .build();
    cluster.execute(list);
};
