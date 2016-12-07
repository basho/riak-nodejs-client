'use strict';

var rpb = require('../../protobuf/riakprotobuf');
var TsColumnType = rpb.getProtoFor('TsColumnType');

var validColumnTypes = Object.freeze([0, 1, 2, 3, 4, 5]);

var ColumnType = Object.freeze({
    Varchar:   TsColumnType.VARCHAR,
    Int64:     TsColumnType.SINT64,
    Double:    TsColumnType.DOUBLE,
    Timestamp: TsColumnType.TIMESTAMP,
    Boolean:   TsColumnType.BOOLEAN,
    Blob:      TsColumnType.BLOB
});

module.exports.ColumnType = ColumnType;
module.exports.validColumnTypes = validColumnTypes;
