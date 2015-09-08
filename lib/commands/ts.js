var tsdata = require('./ts/data');

/**
 * Provides all the commands for Riak Timeseries operations.
 * @module TS
 * @main TS
 */
function TS() { }

// TS exports
module.exports = TS;
module.exports.StoreValue = require('./ts/storevalue');
module.exports.Query = require('./ts/query');
module.exports.ColumnType = tsdata.ColumnType;
