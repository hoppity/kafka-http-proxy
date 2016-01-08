var proxyquire  = require('proxyquire');
var sinon       = require('sinon');


describe('lib/consumers test', function(){
    var kafkaStub;
    var loggerStub;
    var configStub;
    var libConsumer;
    var spy;

    beforeEach(function() {
        kafkaStub       = sinon.stub();
        loggerStub      = sinon.stub();
        configStub      = sinon.stub();
        libConsumer     = proxyquire(
            '../lib/consumers.js', {
                'kafka-node': kafkaStub,    
                '../logger': { logger : { info: sinon.stub(), debug: sinon.stub(), trace: sinon.stub(), warn: sinon.stub(), error: sinon.stub() } },
                '../config': configStub
            });
    });


    describe('getting an consumer', function() {
        it('will throw and exception if no arguments passed', function() {
            expect( function() { libConsumer.get(); } ).toThrowError('Group and Instance ID or ID required.');
        });

        it('will return err if consumer doesnt exist', function() {
            libConsumer.get('test_group', function(err, data) {
                expect(err).not.toBe(null);
                expect(err.message).toEqual('consumer not found');
            });
        });

        it('will return the matching consumer', function() {
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group',
                autoCommitEnable : 'test-value'
            });

            libConsumer.get('test-group', 'test-id', function(err, data) {
                expect(err).toBe(null);
                expect(data.id).toEqual('test-group/test-id');
            });
        });
    });


    describe('add a new consumer', function() {
        beforeEach(function() {
            configStub.consumer = {
                autoCommitEnable : true
            };
        });

        it('adds a predefined consumer', function() {
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group'
            }, function(err, data){
                expect(err).toBe(null);
                expect(data.id).toEqual('test-group/test-id');
                expect(data.autoOffsetReset).toEqual('largest');
                expect(data.autoCommitEnable).toBe(true);
                expect(data.messages.length).toEqual(0);
                expect(data.topics.length).toEqual(0);
                expect(data.offsetMap.length).toEqual(0);
                expect(data.instance).toBe(undefined);
            });
        });

        it('adds the same consumer with autoCommitEnable set', function() {
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group',
                autoCommitEnable : 'test-value'
            }, function(err, data) {
                expect(data.autoCommitEnable).toBe('test-value');
            });

            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group'
            }, function(err, data){
                expect(err).not.toBe(null);
                expect(err.message).toEqual('Consumer with ID test-group/test-id already exists.');
            });
        });
    });


    describe('get messages', function() {
        var consumer;
        beforeEach(function(){
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group',
                autoCommitEnable: false
            }, function(err, data) {
                    consumer = data;
                    consumer.instance = { paused : false};
            });
        });

        function createSampleMessage(offset) {
            return {
                topic: 'test-topic',
                partition: 0,
                offset: offset,
                key: 'key-1',
                value: 'test-value'
            };
        }

        it('should create the message map from one message', function() {
            var message = createSampleMessage(0);
            consumer.messages = [message];

            expect(consumer.offsetMap.length).toBe(0);
            libConsumer.getMessages(consumer, function(err, data) {
                expect(data.length).toBe(1);
                expect(data[0].topic).toEqual(message.topic);
                expect(data[0].partition).toEqual(message.partition);
                expect(data[0].offset).toEqual(message.offset);
            });
            expect(consumer.offsetMap.length).toBe(1);
            expect(consumer.offsetMap[0].topic).toEqual(message.topic);
            expect(consumer.offsetMap[0].partition).toEqual(message.partition);
            expect(consumer.offsetMap[0].offset).toEqual(message.offset + 1);
            expect(consumer.offsetMap[0].metadata).toEqual('m');
        });


        it('should only return one message if two exist with same topic and partition', function() {
            consumer.messages = [createSampleMessage(0), createSampleMessage(1), createSampleMessage(2)];

            expect(consumer.offsetMap.length).toBe(0);
            libConsumer.getMessages(consumer, function(err, data) {
                expect(data.length).toBe(3);
                expect(data[0].topic).toEqual('test-topic');
                expect(data[0].partition).toEqual(0);
                expect(data[0].offset).toEqual(0);
                expect(data[1].offset).toEqual(1);
                expect(data[2].offset).toEqual(2);
            });
            expect(consumer.offsetMap.length).toBe(1);
            expect(consumer.offsetMap[0].topic).toEqual('test-topic');
            expect(consumer.offsetMap[0].partition).toEqual(0);
            expect(consumer.offsetMap[0].offset).toEqual(3);
            expect(consumer.offsetMap[0].metadata).toEqual('m');
        });
    });


    describe('delete consumers', function() {
        var consumer;

        beforeEach(function(){
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group'
            }, function(err, data) {
                    consumer = data;
            });
        });

        it('should remove the valid consumer', function() {
            libConsumer.delete(consumer);
            libConsumer.get(consumer.id, function(err, data) {
                expect(err).not.toBe(null);
                expect(err.message).toEqual('consumer not found');
            });
        });

        it('should not remove other consumers', function(){
            libConsumer.delete({id : 'sample-id'});
            libConsumer.get(consumer.id, function(err, data) {
                expect(err).toBe(null);
                expect(data.id).toEqual(consumer.id);
            });
        });
    });


    describe('time out consumers', function() {

        var consumer;
        function setupConsumer(timeout) {
            configStub.consumer = { timeoutMs : timeout};
            libConsumer.add({
                instanceId : 'test-id',
                group : 'test-group'
            }, function(err, data){
                consumer = data;
            });
        }

        it('should not delete valid consumers', function(){
            setupConsumer(60);
            libConsumer.timeout();

            libConsumer.get(consumer.group, consumer.instanceId, function(err, data) {
                expect(err).toBe(null);
            });
        });

        it('should delete timed out consumers', function() {
            consumer.lastPoll = new Date('1/1/2000');

            libConsumer.timeout();
            libConsumer.get(consumer.group, consumer.instanceId, function(err, data) {
                expect(err).not.toBe(null);
            });
        });

    });
});
