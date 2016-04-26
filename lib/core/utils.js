'use strict';

var logger = require('winston');
var util = require('util');

var rpb = require('../protobuf/riakprotobuf');
var rpbErrorRespCode = rpb.getCodeFor('RpbErrorResp');

function checkRiakError(d) {
    var msg = null;
    var error = false;
    if (d.code === rpbErrorRespCode) {
        error = true;
        var errmsg = d.decoded.getErrmsg().toString('utf8');
        var errcode = d.decoded.getErrcode();
        msg = util.format("%s command '%s' received RpbErrorResp (%d) %s",
            d.conn.name, d.command.name, errcode, errmsg);
        logger.debug(msg);
        if (d.shouldCallback) {
            d.command.onRiakError(d.decoded);
        }
    }
    return { error: error, riakError: error, msg: msg };
}

function checkRespCode(d) {
    var msg = null;
    var error = false;
    var expectedCode = d.command.getExpectedResponseCode();
    if (d.code !== expectedCode) {
        error = true;
        msg = util.format("%s command '%s' received incorrect response; expected %d, got %d",
            d.conn.name, d.command.name, expectedCode, d.code);
        logger.debug(msg);
        if (d.shouldCallback) {
            d.command.onError(msg);
        }
    }
    return { error: error, riakError: false, msg: msg };
}

function handleRiakResponse(d, onError, onSuccess) {
    var err = checkRiakError(d);
    if (err.error === false) {
        err = checkRespCode(d);
    }
    if (err.error === true) {
        onError(err);
    } else {
        if (d.shouldCallback) {
            d.command.onSuccess(d.decoded);
        }
        onSuccess();
    }
}

function stateCheck(name, state, allowedStates, stateNames) {
    if (allowedStates.indexOf(state) === -1) {
        var sn = allowedStates.map(function (val, idx, ary) {
            return stateNames[val];
        });
        var msg = util.format('%s illegal state! required: %s current: %s',
            name, sn, stateNames[state]);
        throw new Error(msg);
    }
}

module.exports.handleRiakResponse = handleRiakResponse;
module.exports.stateCheck = stateCheck;
