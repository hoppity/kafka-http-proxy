var kafka = require('kafka-node'),
    config = require('../config'),
    consumers = {},
    log = require('../logger'),
    logger = log.logger,
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
        consumer.lock = false;

        consumers[consumer.id] = consumer;
        logger.debug({consumer: consumer}, 'lib/consumerManager : consumer added');
    },

    createConsumerInstance = function (consumer, topic) {
        if (consumer.instance.lock) {
            log.debug('lib/consumerManager : consumer is locked, return as there is already an instance or its being recreated');
            return;
        }

        logger.debug('lib/consumerManager : creating an kafka client');
        var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
        logger.debug('lib/consumerManager : creating a new consumer');
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

        logger.debug('lib/consumerManager : consumer created');
        consumer.instance.on('message', function (m) {
            logger.debug('lib/consumerManager : consumer message received');
            consumer.messages.push(m);
        });

        consumer.instance.on('error', function (e) {
            logger.error({ error: e, consumer: consumer.id }, 'lib/consumerManager : Error in consumer instance. Closing and recreating...');
            if (!consumer.instance.lock) {
                consumer.instance.lock = true;
                logger.debug({ consumer: consumer.id }, 'lock enabled on consumer');
                consumer.instance.close(false, function () {
                    setTimeout(function () {
                        logger.info({consumer: consumer.id}, 'lib/consumerManager : Recreating consumer');
                        createConsumerInstance(consumer, topic);
                        consumer.instance.lock = false;
                        logger.debug({ consumer: consumer.id }, 'lock released on consumer');
                    }, 1000);
                });
            }
        });

        consumer.instance.on('offsetOutOfRange', function (e) {
            logger.warn({ error: e }, 'lib/consumerManager : Received alert for offset out of range.');
        });
    },

    deleteConsumer = function (consumer, cb) {
        logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Removing consumer from set.');
        delete consumers[consumer.id];
        logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Closing consumer...');
        consumer.instance.close(false, function () {
            logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Consumer closed.');
            if (!!cb) cb();
        });
    };

module.exports = {
    get: get,
    add: add,
    createInstance: createConsumerInstance,
    delete: deleteConsumer
};
