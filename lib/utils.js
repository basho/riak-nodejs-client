function isString(v) {
    return (typeof v === 'string' || v instanceof String);
}

module.exports.isString = isString;
