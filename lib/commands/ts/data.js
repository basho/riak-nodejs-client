var rpb = require('../../protobuf/riakprotobuf');
var TsColumnType = rpb.getProtoFor('TsColumnType');

var validColumnTypes = Object.freeze([0, 1, 2, 3, 4, 5, 6]);

var ColumnType = Object.freeze({
    Binary:    TsColumnType.BINARY,
    Integer:   TsColumnType.INTEGER,
    Numeric:   TsColumnType.NUMERIC,
    Timestamp: TsColumnType.TIMESTAMP,
    Boolean:   TsColumnType.BOOLEAN,
    Set:       TsColumnType.SET,
    Map:       TsColumnType.MAP,
});

module.exports.ColumnType = ColumnType;
module.exports.validColumnTypes = validColumnTypes;
