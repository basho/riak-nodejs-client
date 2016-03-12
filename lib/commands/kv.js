'use strict';

/**
 * Provides all the commands for Riak Key-Value operations.
 * @module KV
 * @main KV
 */
function KV() { }

// KV exports
module.exports = KV;
module.exports.DeleteValue = require('./kv/deletevalue');
module.exports.FetchBucketProps = require('./kv/fetchbucketprops');
module.exports.FetchBucketTypeProps = require('./kv/fetchbuckettypeprops');
module.exports.FetchPreflist = require('./kv/fetchpreflist');
module.exports.FetchValue = require('./kv/fetchvalue');
module.exports.ListBuckets = require('./kv/listbuckets');
module.exports.ListKeys = require('./kv/listkeys');
module.exports.RiakObject = require('./kv/riakobject');
module.exports.SecondaryIndexQuery = require('./kv/secondaryindexquery');
module.exports.StoreBucketProps = require('./kv/storebucketprops');
module.exports.StoreBucketTypeProps = require('./kv/storebuckettypeprops');
module.exports.ResetBucketProps = require('./kv/resetbucketprops');
module.exports.StoreValue = require('./kv/storevalue');

