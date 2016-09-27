'use strict';

var logger = require('winston');

// Core modules
var Core = require('./core/core');

// Ping command
var Ping = require('./commands/ping');

// FetchServerInfo command
var FetchServerInfo = require('./commands/fetchserverinfo');

// KV exports / commands
var KV = require('./commands/kv');

// CRDT exports / commands
var CRDT = require('./commands/crdt');

// Yokozuna exports / commands
var YZ = require('./commands/yz');

// Map-Reduce exports / commands
var MR = require('./commands/mr');

// Timeseries exports / commands
var TS = require('./commands/ts');

/**
 * Provides the Client class
 * @module Client
 */

/**
 * A Class that represents a Riak client.
 * 
 * The client constructor accepts a {{#crossLink "RiakCluster"}}{{/crossLink}}, or a list of host[:port] from
 * which it will create a RiakCluster using our default configuration. 
 * 
 *     var Riak = require('basho-riak-client');
 *     var client = new Riak.Client(['192.168.1.1:8087', '192.168.1.2:8087']);
 *
 * Two styles of use are supported. Each command you can send to Riak is a 
 * class and can be instantiated then executed by the client's execute() method.
 * In addition, convenience methods are provided directly by the Client class
 * that corrispond to each of the commands.
 * 
 *     // Using the builder, but options can also be passed directly to the constructor
 *     var fetch = new KV.FetchValue.Builder() 
 *                  .withBucket('myBucket')
 *                  .withKey('myKey')
 *                  .withCallback(callback)
 *                  .build();
 *                  
 *     client.execute(fetch);
 *     
 * or
 * 
 *     client.fetchValue({ bucket: 'myBucket', key: 'myKey' }, callback);
 *     
 * All command callbacks are the typical node.js function(err, resp) style. 
 *
 * @class Client
 * @constructor
 * @param {String[]|RiakCluster} cluster - either an array of host[:port] strings or the cluster to use. See {{#crossLink "RiakCluster"}}{{/crossLink}}.
 * @param {Function} [callback] - called when cluster is started (optional)
 * @param {Object} [callback.err] - set to an error if one occurrs during start.
 * @param {Object} [callback.client] - the client object.
 * @param {Object} [callback.cluster] - the cluster object.
 */
function Client() {
    if (arguments.length === 0) {
        throw new Error('an array of IP|fqdn[:port] or an instance of RiakCluser is required');
    }

    if (arguments[0] instanceof Core.RiakCluster) {
        this.cluster = arguments[0];
    } else if (Array.isArray(arguments[0])) {
        var nodes = Core.RiakNode.buildNodes(arguments[0]);
        this.cluster = new Core.RiakCluster({ nodes: nodes});
    } else {
        throw new Error('an array of IP|fqdn[:port] or an instance of RiakCluser is required');
    }

    var callback = function (err, client, cluster) {
        logger.debug('[Client] cluster is started.');
    };

    if (arguments.length === 2 && typeof arguments[1] === 'function') {
        callback = arguments[1];
    }

    var self = this;
    this.cluster.start(function (err, cluster) {
        callback(err, self, cluster);
    });
}

/**
 * Shut down the client gracefully.
 * 
 * This will cause all connections to be closed and any remaining in-flight
 * commands to finish. 
 * 
 * The provided callback will be added as a stateChange listener to the client's
 * RiakCluster.
 * 
 * See: {{#crossLink "RiakCluster/stateChange:event"}}RiakCluster.stateChange{{/crossLink}}
 * 
 * @method shutdown
 * @param {Function} callback - will be registered as a stateChange listener on the RiakCluster.
 */
Client.prototype.shutdown = function(callback) {
    logger.debug('[Client] shutting down cluster.');
    this.cluster.on('stateChange', callback);
    this.cluster.stop();
};

/**
 * Stop the client gracefully.
 * 
 * This will cause all connections to be closed and any remaining in-flight
 * commands to finish. 
 * 
 * The provided callback will be called when the client is completely stopped.
 * 
 * @method stop
 * @param {Function} callback - will be called when client is stopped.
 * @param {Object} [callback.err] - set to an error if one occurrs during stop.
 * @param {Object} [callback.state] - the state of the cluster at shutdown.
 */
Client.prototype.stop = function(callback) {
    logger.debug('[Client] stopping cluster.');
    this.cluster.stop(callback);
};

/**
 * Get the client's RiakCluster.
 * 
 * @method getRiakCluster
 * @return {Core.RiakCluster}
 */
Client.prototype.getRiakCluster = function() {
    return this.cluster;
};

/*
 * KV methods
 */

/**
 * See {{#crossLink "RiakCluster/execute:method"}}RiakCluster#execute{{/crossLink}}
 * @method execute
 * @param {Object} command Any Riak command object from the various modules.
 */
Client.prototype.execute = function(command) {
    this.cluster.execute(command);
};

/**
 * See {{#crossLink "Ping"}}{{/crossLink}}
 * @method ping
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.ping = function(callback) {
    var cmd = new Ping(callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchServerInfo"}}{{/crossLink}}
 * @method fetchServerInfo
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchServerInfo = function(callback) {
    var cmd = new FetchServerInfo(callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "DeleteValue"}}{{/crossLink}}
 * @method deleteValue
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.deleteValue = function(options, callback) {
    var cmd = new KV.DeleteValue(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchBucketProps"}}{{/crossLink}}
 * @method fetchBucketProps 
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchBucketProps = function(options, callback) {
    var cmd = new KV.FetchBucketProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchBucketTypeProps"}}{{/crossLink}}
 * @method fetchBucketTypeProps
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchBucketTypeProps = function(options, callback) {
    var cmd = new KV.FetchBucketTypeProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchValue"}}{{/crossLink}}
 * @method fetchValue
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchValue = function(options, callback) {
    var cmd = new KV.FetchValue(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchPreflist"}}{{/crossLink}}
 * @method fetchPreflist
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchPreflist = function(options, callback) {
    var cmd = new KV.FetchPreflist(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListBuckets"}}{{/crossLink}}
 * @method ListBuckets
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.listBuckets = function(options, callback) {
    var cmd = new KV.ListBuckets(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListKeys"}}{{/crossLink}}
 * @method listKeys
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.listKeys = function(options, callback) {
    var cmd = new KV.ListKeys(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "SecondaryIndexQuery"}}{{/crossLink}}
 * @method secondaryIndexQuery
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.secondaryIndexQuery = function(options, callback) {
    var cmd = new KV.SecondaryIndexQuery(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreBucketProps"}}{{/crossLink}}
 * @method storeBucketProps
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.storeBucketProps = function(options, callback) {
    var cmd = new KV.StoreBucketProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ResetBucketProps"}}{{/crossLink}}
 * @method resetBucketProps
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.resetBucketProps = function(options, callback) {
    var cmd = new KV.ResetBucketProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreBucketTypeProps"}}{{/crossLink}}
 * @method storeBucketTypeProps
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.storeBucketTypeProps = function(options, callback) {
    var cmd = new KV.StoreBucketTypeProps(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreValue"}}{{/crossLink}}
 * @method storeValue
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.storeValue = function(options, callback) {
    var cmd = new KV.StoreValue(options, callback);
    this.cluster.execute(cmd);
};

/*
 * CRDT methods
 */

/**
 * See {{#crossLink "FetchCounter"}}{{/crossLink}}
 * @method fetchCounter
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchCounter = function(options, callback) {
    var cmd = new CRDT.FetchCounter(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "UpdateCounter"}}{{/crossLink}}
 * @method updateCounter
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.updateCounter = function(options, callback) {
    var cmd = new CRDT.UpdateCounter(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchSet"}}{{/crossLink}}
 * @method fetchSet
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchSet = function(options, callback) {
    var cmd = new CRDT.FetchSet(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "UpdateSet"}}{{/crossLink}}
 * @method updateSet
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.updateSet = function(options, callback) {
    var cmd = new CRDT.UpdateSet(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchMap"}}{{/crossLink}}
 * @method fetchMap
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchMap = function(options, callback) {
    var cmd = new CRDT.FetchMap(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "UpdateMap"}}{{/crossLink}}
 * @method updateMap
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.updateMap = function(options, callback) {
    var cmd = new CRDT.UpdateMap(options, callback);
    this.cluster.execute(cmd);
};

/*
 * YZ methods
 */

/**
 * See {{#crossLink "DeleteIndex"}}{{/crossLink}}
 * @method deleteIndex
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.deleteIndex = function(options, callback) {
    var cmd = new YZ.DeleteIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchIndex"}}{{/crossLink}}
 * @method fetchIndex
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchIndex = function(options, callback) {
    var cmd = new YZ.FetchIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "FetchSchema"}}{{/crossLink}}
 * @method fetchSchema
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.fetchSchema = function(options, callback) {
    var cmd = new YZ.FetchSchema(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Search"}}{{/crossLink}}
 * @method search
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.search = function(options, callback) {
    var cmd = new YZ.Search(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreIndex"}}{{/crossLink}}
 * @method storeIndex
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.storeIndex = function(options, callback) {
    var cmd = new YZ.StoreIndex(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "StoreSchema"}}{{/crossLink}}
 * @method storeSchema
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 */
Client.prototype.storeSchema = function(options, callback) {
    var cmd = new YZ.StoreSchema(options, callback);
    this.cluster.execute(cmd);
};

/*
 * MR 
 */

/**
 * See {{#crossLink "MapReduce"}}{{/crossLink}}
 * @method mapReduce
 * @param {String} query The map-reduce query. 
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {Boolean} [stream=true] Stream the results or accumulate before calling callback.
 * 
 */
Client.prototype.mapReduce = function(query, callback, stream) {
    var cmd = new MR.MapReduce(query, callback, stream);
    this.cluster.execute(cmd);
};

/*
 * TS
 */

/**
 * See {{#crossLink "Store"}}{{/crossLink}}
 * @method tsStore
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsStore = function(options, callback) {
    var cmd = new TS.Store(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Describe"}}{{/crossLink}}
 * @method tsDescribe
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsDescribe = function(options, callback) {
    var cmd = new TS.Describe(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Query"}}{{/crossLink}}
 * @method tsQuery
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsQuery = function(options, callback) {
    var cmd = new TS.Query(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Get"}}{{/crossLink}}
 * @method tsGet
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsGet = function(options, callback) {
    var cmd = new TS.Get(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "Delete"}}{{/crossLink}}
 * @method tsDelete
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsDelete = function(options, callback) {
    var cmd = new TS.Delete(options, callback);
    this.cluster.execute(cmd);
};

/**
 * See {{#crossLink "ListKeys"}}{{/crossLink}}
 * @method tsListKeys
 * @param {Object} options The options for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * 
 */
Client.prototype.tsListKeys = function(options, callback) {
    var cmd = new TS.ListKeys(options, callback);
    this.cluster.execute(cmd);
};

// Namespaces
function Riak() { }
function Commands() { }

// Core exports
module.exports = Riak;
module.exports.Client = Client;

module.exports.Node = Core.RiakNode;
module.exports.Cluster = Core.RiakCluster;
module.exports.Cluster.DefaultNodeManager = Core.DefaultNodeManager;
module.exports.Cluster.RoundRobinNodeManager = Core.RoundRobinNodeManager;
module.exports.Cluster.LeastExecutingNodeManager = Core.LeastExecutingNodeManager;

// Command exports
module.exports.Commands = Commands;
module.exports.Commands.Ping = Ping;
module.exports.Commands.FetchServerInfo = FetchServerInfo;

module.exports.Commands.KV = KV;
module.exports.Commands.CRDT = CRDT;
module.exports.Commands.YZ = YZ;
module.exports.Commands.MR = MR;
module.exports.Commands.TS = TS;

