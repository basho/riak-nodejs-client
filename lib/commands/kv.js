/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
module.exports.StoreValue = require('./kv/storevalue');

