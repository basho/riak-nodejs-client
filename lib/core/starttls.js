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

var RiakProtoBuf = require('../protobuf/riakprotobuf');
var requestCode = RiakProtoBuf.getCodeFor('RpbStartTls');
var expectedCode = requestCode;
var CommandBase = require('../commands/commandbase');
var inherits = require('util').inherits;

/**
 * Provides the StartTls class and its response.
 * @module StartTls
 *
 */

/**
 * Command used to start a TLS session with Riak.
 * 
 * @class StartTls
 * @constructor
 * @extends CommandBase
 * 
 */ 
function StartTls() {
    CommandBase.call(this, 'RpbStartTls', 'RpbStartTls');
}

inherits(StartTls, CommandBase);

StartTls.prototype.constructPbRequest = function() {
    
    var protobuf = this.getPbReqBuilder();

    /*
     * TODO since this is just a message code, what should go here?
     */
    
    return protobuf;

};

StartTls.prototype.onSuccess = function(rpbGetResp) {
        
    if (rpbGetResp === null) {
        // TODO ERROR
    } else {

        var pbContentArray = rpbGetResp.getContent();
        // var vclock = rpbGetResp.getVclock().toBuffer();

        var riakMeta, riakValue;
        if (pbContentArray.length === 0) {
            riakMeta = new RiakMeta();

            riakValue = new Buffer(0);
            riakMeta.isTombstone = true;
            riakMeta.key = this.key;
            riakMeta.bucket = this.bucket;
            riakMeta.bucketType = this.bucketType;
            this.callback(false, false, [new KvResponseBase.KvValueMetaPair(riakValue, riakMeta)]);
        } else {

            var values = new Array(pbContentArray.length);

            for (var i = 0; i < pbContentArray.length; i++) {
                riakMeta = RiakMeta.extractMetaFromRpbContent(pbContentArray[i], vclock, this.bucketType, this.bucket, this.key);
                riakValue = pbContentArray[i].getValue().toBuffer();
                values[i] = new KvResponseBase.KvValueMetaPair(riakValue, riakMeta);
            }

            this.callback(null, new Response(false, false, values));
        }
    }

    return true;
};
    
StartTls.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StartTls.prototype.onError = function(msg) {
    this.callback(msg, null);
};

/**
 * The response for a StartTls command.
 * @namespace StartTls
 * @class Response
 * @constructor
 * @param {Boolean} notFound if the response was not found.
 * @param {Boolean} unchanged if the response was unchanged.
 * @param {KvValueMetaPair[]} valueMetaPairs array of KvValueMetaPairs from Riak.
 * @extends KvResponseBase
 */
function Response(notFound, unchanged, valueMetaPairs) {
    
    KvResponseBase.call(this, valueMetaPairs);
    this.notFound = notFound;
    this.unchanged = unchanged;
}

inherits(Response, KvResponseBase);

/**
 * Determine if a value was present in Riak.
 * @return {Boolean} true if there was no value in riak. 
 */
Response.prototype.isNotFound = function() {
    return this.notFound;
};

/**
* Determine if the value is unchanged.
* 
* If the fetch request included a vclock via withIfNotModified()
* this indicates if the value in Riak has been modified. 
*
* @return {Boolean} true if the vector clock for the object in Riak matched the supplied vector clock, false otherwise.
*/
Response.prototype.isUnchanged = function() {
    return this.unchanged;
};


module.exports = StartTls;

