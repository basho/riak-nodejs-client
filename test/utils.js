/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

function includesBuffer(haystack, needle) {
    var needleBuf = new Buffer(needle);
    var len = haystack.length;
    for (var i = 0; i < len; i++) {
        if (haystack[i].equals(needleBuf)) return true;
    }
    
    return false;
}

function includes(haystack, needle) {
    var len = haystack.length;
    for (var i = 0; i < len; i++) {
        if (haystack[i] === needle) return true;
    }

    return false;
}

module.exports.includesBuffer = includesBuffer;
module.exports.includes = includes;
