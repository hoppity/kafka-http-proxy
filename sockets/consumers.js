var logger = require('../logger.js').logger;
var uuid = require('uuid');
var config = require('../config.js');
var kafka = require('kafka-node');

module.exports = function (server) {
    var io = require('socket.io')(server);

    logger.info('initialising socket.io');

    io.on('connection', function (socket) {
        socket.uuid = uuid.v4();
        socket.topics = [];

        logger.info({socket: socket.uuid}, 'sockets/consumer : connection established');

        socket.on('subscribe', function (data) {
            logger.info({data: data}, 'sockets/consumer : subscribe received');
            var group = data.group;
            var topic = data.topic;

            if (!socket.consumer) {

                logger.debug('sockets/consumers : creating a kafka client');
                var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
                
                logger.debug('sockets/consumers : creating a new consumer');
                var consumer = new kafka.HighLevelConsumer(client, [{
                    topic: topic
                }], {
                    groupId: group,
                    // Auto commit config
                    autoCommit: true,
                    autoCommitIntervalMs: 5000,
                    // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
                    fetchMaxWaitMs: 100,
                    // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
                    fetchMinBytes: 1,
                    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
                    fetchMaxBytes: 4 * 1024 * 1024, // 4MB
                    // If set true, consumer will fetch message from the given offset in the payloads
                    fromOffset: false,
                    // If set to 'buffer', values will be returned as raw buffer objects.
                    encoding: 'utf8'
                });
                socket.consumer = consumer;

                logger.debug({socket:socket.uuid, consumer:consumer.id}, 'sockets/consumers : consumer created');
                socket.consumer.on('message', function (m) {
                    logger.trace(m, 'sockets/consumers : message received');
                    socket.emit('message', m);
                });

                socket.consumer.on('error', function (e) {
                    logger.error({ error: e, consumer: consumer.id }, 'lib/consumerManager : Error in consumer instance. Closing and recreating...');
                    //TODO: close and recreate
                });

                socket.consumer.on('offsetOutOfRange', function (e) {
                    logger.warn({ error: e }, 'lib/consumerManager : Received alert for offset out of range.');
                });
            }
        });

        socket.on('disconnect', function (data) {
            if (socket.consumer) {
                socket.consumer.close(false);
            }

            logger.info({socket:socket.uuid}, 'sockets/consumer : disconnected');
        });
    });
};