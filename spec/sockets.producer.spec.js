var proxyquire  = require('proxyquire');
var sinon       = require('sinon');


describe('sockes/producer tests', function() {
    var serverStub;
    var configStub;
    var kafkaStub;
    var loggerStub;
    var producerStub;
    var libProducersStub;
    var socketIoStub;
    var socketStub;
    var producer;

    beforeEach(function() {
        serverStub          = sinon.stub();
        configStub          = sinon.stub();
        kafkaStub           = sinon.stub();
        loggerStub          = sinon.stub();
        producerStub        = {
                                on: sinon.stub(),
                                close: sinon.spy(),
                                ready: sinon.stub().returns(true), 
                                createTopics: sinon.spy()
                              };
        libProducersStub    = {
                                create: sinon.stub().returns(producerStub),
                                publish: sinon.stub()
                              };
        ioStub              = { on: sinon.stub(), emit: sinon.stub() };
        socketStub          = { on: sinon.stub(), emit: sinon.stub() };
        socketIoStub        = sinon.stub().returns(ioStub);
        producer            = proxyquire(
            '../sockets/producer',
            {
                '../config'         : configStub,
                '../logger'         : { logger : { info: sinon.stub(), debug: sinon.stub(), trace: sinon.stub(), warn: sinon.stub(), error: sinon.stub() } },
                '../lib/producers'  : libProducersStub,
                'kafka-node'        : kafkaStub,
                'socket.io'         : socketIoStub
            });
    });

    describe('when initialising', function() {
        beforeEach(function () {
            producer(serverStub);
        });

        it('should setup listener on /sockets/producer', function() {
            expect(socketIoStub.calledWith(serverStub, { path: '/sockets/producer' })).toBe(true);
        });

        it('should listen for connection events', function () {
            expect(ioStub.on.calledWith('connection')).toBe(true);
        });
    });

    describe('when new connection openned', function () {
        beforeEach(function () {
            ioStub.on.onFirstCall().callsArgWith(1, socketStub);

            producer(serverStub);
        });

        it('should initialise producer and subscribe to producer events', function () {
            expect(socketStub.uuid).not.toBe(undefined);
            expect(producerStub.on.calledWith('ready')).toBe(true);
            expect(producerStub.on.calledWith('error')).toBe(true);
        });

        it('should subscribe to socket events', function () {
            expect(socketStub.on.calledWith('disconnect')).toBe(true);
            expect(socketStub.on.calledWith('createTopic')).toBe(true);
            expect(socketStub.on.calledWith('publish')).toBe(true);
        });
    });

    describe('when producer ready received', function () {
        // first call of "on" should be socket.producer.on('ready', callback)
        beforeEach(function () {
            ioStub.on.onFirstCall().callsArgWith(1, socketStub);
            producerStub.on.onFirstCall().callsArg(1);

            producer(serverStub);
        });

        it('should emit ready event on socket', function () {
            expect(socketStub.emit.calledWith('ready')).toBe(true);
        });
    });

    describe('when producer error received', function () {
        // second call of "on" should be socket.producer.on('error', callback)
        beforeEach(function () {
            ioStub.on.onFirstCall().callsArgWith(1, socketStub);
            producerStub.on.onSecondCall().callsArg(1);

            producer(serverStub);
        });

        it('should emit error event on socket', function () {
            expect(socketStub.emit.calledWith('error')).toBe(true);
        });
    });

    describe('when socket disconnect received', function () {
        // first call of "on" should be socket.on('disconnect', callback)
        beforeEach(function () {
            ioStub.on.onFirstCall().callsArgWith(1, socketStub);
            socketStub.on.onFirstCall().callsArg(1);

            producerStub.close = sinon.spy();

            producer(serverStub);
        });

        it('should close producer', function () {
            expect(producerStub.close.called).toBe(true);
        });
    });
    
});
