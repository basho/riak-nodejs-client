'use strict';

var tsdata = require('./ts/data');

/**
 * Provides all the commands for Riak Timeseries operations.
 * @module TS
 * @main TS
 */
function TS() { }

// TS exports
module.exports = TS;
module.exports.Describe = require('./ts/describe');
module.exports.Store = require('./ts/store');
module.exports.Query = require('./ts/query');
module.exports.Get = require('./ts/get');
module.exports.Delete = require('./ts/delete');
module.exports.ListKeys = require('./ts/listkeys');
module.exports.ColumnType = tsdata.ColumnType;
