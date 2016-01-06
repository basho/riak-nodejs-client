var assert = require('assert');
var AuthReq = require('../../../lib/core/authreq');

describe('AuthReq', function() {
    it('sets user and password from options', function(done) {
        var user = 'user';
        var password = 'password';
        var opts = {
            user: user,
            password: password
        };
        var cb = function (e, r) { };
        var r = new AuthReq(opts, cb);
        assert.strictEqual(r.user, user);
        assert.strictEqual(r.password, password);
        done();
    });
});
