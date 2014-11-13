/*
 * Copyright 2014 Basho Technologies, Inc.
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

var RpbGetReq = require('../protobuf/riakprotobuf').getProtoFor('RpbGetReq');
var requestCode = require('../protobuf/riakprotobuf').getCodeFor('RpbGetReq');
var expectedCode = require('../protobuf/riakprotobuf').getCodeFor('RpbGetResp');

function FetchValue(builder) {
    
    this.protobuf = new RpbGetReq();
    this.protobuf.setBucket(new Buffer('my_bucket'));
    this.protobuf.setKey(new Buffer('my_key'));
    this.callback = function(err, response) {
        if (err)
            console.log(err);
        else
            console.log(response);
    };
    this.streaming = false;
    this.header = new Buffer(5);
    this.header.writeUInt8(requestCode, 4);
    this.remainingTries = 1;
}

FetchValue.prototype = {
    getRiakMessage : function() {
        var encoded = this.protobuf.encode().toBuffer();
        this.header.writeInt32BE(encoded.length + 1, 0);
        return {
            protobuf: encoded,
            header: this.header
        };
    },
    getExpectedResponseCode : function() {
        return expectedCode;
    },
    onSuccess : function(rpbGetResp) {
        this.callback(null, rpbGetResp);
        return true;
    },
    onRiakError : function(rpbErrorResp) {
        this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
    },
    onError : function(msg) {
        this.callback(msg, null);
    }
    
};

module.exports = FetchValue;