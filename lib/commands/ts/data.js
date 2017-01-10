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
