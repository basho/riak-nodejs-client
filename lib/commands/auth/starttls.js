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

var RiakProtoBuf = require('../protobuf/riakprotobuf'),
    requestCode = RiakProtoBuf.getCodeFor('RpbStartTls'),
    expectedCode = RiakProtoBuf.getCodeFor('RpbStartTls');

function StartTls(builder) {
    
    this.protobuf = new RpbStartTlsReq();
    this.protobuf.setBucket(new Buffer('my_bucket'));
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

StartTls.prototype = {
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
    onSuccess : function(RpbStartTlsResp) {
        this.callback(null, RpbStartTlsResp);
        return RpbStartTlsResp.done;
    },
    onRiakError : function(rpbErrorResp) {
        this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
    },
    onError : function(msg) {
        this.callback(msg, null);
    }
    
};

module.exports = StartTls;
