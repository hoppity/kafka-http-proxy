var kafka = require('kafka-node'),
    config = require('../config'),
    consumers = new Object(),
    logger = require('../logger'),
    uuid = require('uuid'),

    get = function () {
        if (arguments.length < 1) throw 'Group and Instance ID or ID required.';

        var id = arguments[0];
        if (arguments.length > 1) id += '/' + arguments[1];

        return consumers[id];
    },

    add = function (consumer) {
        consumer.instanceId = consumer.instanceId || uuid.v4();
        consumer.id = consumer.group + '/' + consumer.instanceId;
        if (!!consumers[consumer.id]) throw 'Consumer with ID ' + consumer.id + ' already exists.';

        consumer.autoOffsetReset = consumer.autoOffsetReset || 'largest';
        consumer.autoCommitEnable = typeof consumer.autoCommitEnable === 'undefined' ? 'true' : consumer.autoCommitEnable;
        consumer.instance = undefined;
        consumer.topics = [];
        consumer.messages = [];
        consumer.created = new Date();
        consumer.lastPoll = Date.now();

        consumers[consumer.id] = consumer;
    }

    createConsumerInstance = function (consumer, topic) {
        var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
        consumer.instance = new kafka.HighLevelConsumer(client, [{
            topic: topic
        }], {
            groupId: consumer.group,
            // Auto commit config 
            autoCommit: false,
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
        consumer.instance.on('message', function (m) {
            consumer.messages.push(m);
        });
        consumer.instance.on('error', function (e) {
            logger.error({ error: e, consumer: consumer.id }, 'Error in consumer instance. Closing and recreating...');
            consumer.instance.close(false, function () {
                setTimeout(function () {
                    logger.debug({consumer: consumer.id}, 'Recreating consumer');
                    createConsumerInstance(consumer, topic);
                }, 1000);
            });
        });
        consumer.instance.on('offsetOutOfRange', function (e) {
            logger.warn({ error: e }, 'Received alert for offset out of range.');
        });
    },

    deleteConsumer = function (consumer, cb) {
        logger.debug({ consumer: consumer.id }, 'Removing consumer from set.');
        delete consumers[consumer.id];
        logger.debug({ consumer: consumer.id }, 'Closing consumer...');
        consumer.instance.close(false, function () {
            logger.debug({ consumer: consumer.id }, 'Consumer closed.')
            if (!!cb) cb();
        });
    };

module.exports = {
    get: get,
    add: add,
    createInstance: createConsumerInstance,
    delete: deleteConsumer
};
