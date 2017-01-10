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

var tsdata = require('./ts/data');

/**
 * Provides all the commands for Riak Timeseries operations.
 * @module TS
 * @main TS
 */
function TS() { }

// TS exports
module.exports = TS;
module.exports.Describe = require('./ts/describe');
module.exports.Store = require('./ts/store');
module.exports.Query = require('./ts/query');
module.exports.Get = require('./ts/get');
module.exports.Delete = require('./ts/delete');
module.exports.ListKeys = require('./ts/listkeys');
module.exports.ColumnType = tsdata.ColumnType;
