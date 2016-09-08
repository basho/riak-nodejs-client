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
    var asNum = parseFloat(n);
    if (isNaN(asNum)) {
        return undefined;
    }
    return asNum;
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

module.exports.isString = isString;
module.exports.isNullOrUndefined = isNullOrUndefined;
module.exports.maybeIsNumber = maybeIsNumber;
module.exports.isInteger = isInteger;
module.exports.isFloat = isFloat;
module.exports.bb = bb;
