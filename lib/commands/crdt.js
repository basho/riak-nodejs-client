'use strict';

/**
 * Provides all the commands for Riak CRDTs (Conflict-Free Replicated Data Type)
 * @module CRDT
 * @main CRDT
 */
function CRDT() { }

// CRDT exports
module.exports = CRDT;
module.exports.FetchCounter =  require('./crdt/fetchcounter');
module.exports.UpdateCounter =  require('./crdt/updatecounter');
module.exports.FetchMap =  require('./crdt/fetchmap');
module.exports.UpdateMap =  require('./crdt/updatemap');
module.exports.FetchSet =  require('./crdt/fetchset');
module.exports.UpdateSet =  require('./crdt/updateset');
module.exports.FetchHll =  require('./crdt/fetchhll');
module.exports.UpdateHll =  require('./crdt/updatehll');
