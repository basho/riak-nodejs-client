'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the MapReduce class.
 * @module MR
 */

/**
 * Command used to perform a Map-Reduce query in Riak.
 *
 * The Riak Map-Reduce API uses JSON for its query.
 *
 * A typical map-reduce query (JSON) will look like:
 *
 *     {
 *       "inputs": "goog",
 *       "query": [
 *         {
 *            "map": {
 *              "language": "javascript",
 *              "source": "function(value, keyData, arg){ var data = Riak.mapValuesJson(value)[0]; var month = value.key.split('-').slice(0,2).join('-'); var obj = {}; obj[month] = data.High - data.Low; return [ obj ];}"
 *            }
 *         },
 *         {
 *            "reduce": {
 *              "language": "javascript",
 *              "source": "function(values, arg){ return [ values.reduce(function(acc, item){ for(var month in item){ if(acc[month]) { acc[month] = (acc[month] < item[month]) ? item[month] : acc[month]; } else { acc[month] = item[month]; } } return acc;  }) ];}",
 *              "keep": true
 *            }
 *         }
 *       ]
 *     }
 *
 * For more info see:
 * [Loading Data and Running MapReduce](http://docs.basho.com/riak/latest/tutorials/fast-track/Loading-Data-and-Running-MapReduce-Queries/)
 *
 * @class MapReduce
 * @constructor
 * @param {String} query The Map-Reduce query. This is a string containing JSON.
 * @param {Function} callback The callback to be executed by this command.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the response from Riak.
 * @param {Boolean} callback.response.done True if the entire response has been received.
 * @param {Number} callback.response.phase The phase the response is from.
 * @param {Object[]} callback.response.response The results.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @param {Boolean} [stream=true] stream the results or accumulate before calling callback.
 * @extends CommandBase
 */
function MapReduce(query, callback, stream) {
    CommandBase.call(this, 'RpbMapRedReq', 'RpbMapRedResp', callback);
    var options = { query: query, stream: stream };
    this.validateOptions(options, schema);
    this.query = this.options.query;
    this.stream = this.options.stream;
    if (!this.stream) {
        this.response = {};
    }
}

inherits(MapReduce, CommandBase);

MapReduce.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setContentType(new Buffer('application/json'));
    protobuf.setRequest(new Buffer(this.query));

    return protobuf;

};

MapReduce.prototype.onSuccess = function(rpbMapRedResp) {

     var done = rpbMapRedResp.done ? true : false;
    // The last message is usually empty with only 'done' when streaming
    var responseArray = null;
    var phase = null;
    if (rpbMapRedResp.getResponse()) {
        responseArray = JSON.parse(rpbMapRedResp.getResponse().toString('utf8'));
        phase = rpbMapRedResp.phase ? rpbMapRedResp.phase : 0;

        if (!this.stream) {
            if (!this.response['phase_' + phase]) {
                this.response['phase_' + phase] = responseArray;
            } else {
                Array.prototype.push.apply(this.response['phase_' + phase], responseArray);
            }
        }
    }

    if (!this.stream) {
        if (done) {
            this.response.done = true;
            this._callback(null, this.response);
        }
    } else {
        this._callback(null, { phase : phase, response: responseArray, done: done});
    }

    return done;

};

var schema = Joi.object().keys({
    query: Joi.string().required(),
    stream: Joi.boolean().default(true).optional()
});

module.exports = MapReduce;

