'use strict';

function isString(v) {
    return (typeof v === 'string' || v instanceof String);
}

function isNullOrUndefined(v) {
    return (v === null || typeof v === 'undefined');
}

module.exports.isString = isString;
module.exports.isNullOrUndefined = isNullOrUndefined;
