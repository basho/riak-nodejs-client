'use strict';

/**
 * Provides all the commands for Riak Search 2.0 (Yokozuna/Solr)
 * @module YZ
 * @main YZ
 */
function YZ() { }

// YZ exports
module.exports = YZ;
module.exports.DeleteIndex = require('./yokozuna/deleteindex');
module.exports.FetchIndex = require('./yokozuna/fetchindex');
module.exports.FetchSchema = require('./yokozuna/fetchschema');
module.exports.Search = require('./yokozuna/search');
module.exports.StoreIndex = require('./yokozuna/storeindex');
module.exports.StoreSchema = require('./yokozuna/storeschema');
