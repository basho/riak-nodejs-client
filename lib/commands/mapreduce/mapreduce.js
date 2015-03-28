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


var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the MapReduce class.
 * @module MapReduce
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
 * @param {String} query the Map-Reduce query. 
 * @param {Function} callback the callback to be executed by this command. 
 * @param {String} callback.err error message
 * @param {Object} callback.response the response from Riak. 
 * @param {Boolean} callback.done true if the entire response has been received
 * @param {Boolean} [stream=true] stream the results or accumulate before calling callback.
 * @extends CommandBase
 */
function MapReduce(query, callback, stream) {
    CommandBase.call(this, 'RpbMapRedReq', 'RpbMapRedResp');
    
    var self = this;
    Joi.validate({ query: query, callback: callback, stream: stream}, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.query = options.query;
        self.callback = options.callback;
        self.stream = options.stream;
    });
    
    this.remainingTries = 1;
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
            this.callback(null, this.response);
        }
    } else {
        this.callback(null, { phase : phase, response: responseArray, done: done });
    }
    
    return done;
    
};

MapReduce.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
MapReduce.prototype.onError = function(msg) {
    this.callback(msg, null);
};


var schema = Joi.object().keys({
    query: Joi.string().required(),
    callback: Joi.func().required(),
    stream: Joi.boolean().default(true).optional()
});

module.exports = MapReduce;
