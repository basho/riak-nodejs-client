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

var Long = require('long');

function isString(v) {
    return (typeof v === 'string' || v instanceof String);
}

function isNullOrUndefined(v) {
    return (v === null || typeof v === 'undefined');
}

function maybeIsNumber(n) {
    if (n instanceof Long) {
        return n;
    }
    var regPos = /^\d+(\.\d+)?$/; //Float number
    var regNeg = /^(-(([0-9]+\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\.[0-9]+)|([0-9]*[1-9][0-9]*)))$/;//Float number below zero
    if (regPos.test(n) || regNeg.test(n)) {
    	var asNum = parseFloat(n);
        if (isNaN(asNum)) {
            return undefined;
        }
        return asNum;
    }
    return n;
}

function isInteger(n) {
    if (n instanceof Long) {
        return true;
    } else {
        return typeof n === 'number' && n % 1 === 0;
    }
}

function isFloat(n) {
    return typeof n === 'number' && n % 1 !== 0;
}

function firstUc(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function bb(obj, prop) {
    obj.prototype['with' + firstUc(prop)] = function(pv) {
        this[prop] = pv;
        return this;
    };
}

function ListError() {
    return new Error('Bucket and key list operations are expensive and should not be used in production.');
}

module.exports.isString = isString;
module.exports.isNullOrUndefined = isNullOrUndefined;
module.exports.maybeIsNumber = maybeIsNumber;
module.exports.isInteger = isInteger;
module.exports.isFloat = isFloat;
module.exports.bb = bb;
module.exports.ListError = ListError;
