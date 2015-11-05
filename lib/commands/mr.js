'use strict';

/**
 * Provides the commands for Riak Map-Reduce
 * @module MR
 * @main MR
 */
function MR() { }

// MR exports
module.exports = MR;
module.exports.MapReduce = require('./mapreduce/mapreduce');

