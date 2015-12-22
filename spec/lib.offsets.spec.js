var proxyquire  = require('proxyquire');
var sinon       = require('sinon');


describe('lib/offsets tests', function() {
    var configStub;
    var kafkaStub;
    var loggerStub;
    var libOffset;

    beforeEach(function(){
        configStub      = sinon.stub();
        kafkaStub       = sinon.stub();
        loggerStub      = sinon.stub();
        libOffset       = proxyquire('../lib/offsets.js', { '../config': configStub, '../logger': loggerStub, 'kafka-node': kafkaStub });
    });

    describe('committing offsets', function() {
        it('should not commit empty offset arrays', function() {
            libOffset.commitOffsets('test-group', [], function(err, data) {
                expect(err).not.toBe(null);
                expect(err.message).toEqual('no offsets found to commit');
            });
        });
    });

});
