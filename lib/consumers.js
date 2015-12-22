var kafka       = require('kafka-node'),
    uuid        = require('uuid'),
    config      = require('../config'),
    logger      = require('../logger').logger,
    consumers   = {};



function createConsumer(group, topic, autocommit) {
    var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http);
    return new kafka.HighLevelConsumer(client, [{
        topic: topic
    }], {
        groupId: group,
        // Auto commit config
        autoCommit: autocommit,
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
}

var get = function (group, id, callback) {
        if (arguments.length < 1) {
            throw new Error('Group and Instance ID or ID required.');
        }

        if (typeof id === 'string'){
            group += '/' + id;
        }
        else if (typeof id === 'function') {
            callback = id;
        }

        var consumer = consumers[group];
        if (!consumer) {
            return callback( {message: 'consumer not found'}, null);
        }

        if (callback) {
            return callback(null, consumer);
        }

        return consumer;
    },

    add = function (consumer, callback) {
        consumer.instanceId = consumer.instanceId || uuid.v4();
        consumer.id = consumer.group + '/' + consumer.instanceId;
        if (!!consumers[consumer.id]) {
            return callback({
                name: 'Error',
                message: 'Consumer with ID ' + consumer.id + ' already exists.'});
        }

        consumer.autoOffsetReset = consumer.autoOffsetReset || 'largest';
        consumer.autoCommitEnable = typeof consumer.autoCommitEnable === 'undefined' ? config.consumer.autoCommitEnable : consumer.autoCommitEnable;
        consumer.instance = undefined;
        consumer.topics = [];
        consumer.messages = [];
        consumer.created = new Date();
        consumer.lastPoll = Date.now();
        consumer.lock = false;
        consumer.byteSize = 0;
        consumer.offsetMap = [];

        consumers[consumer.id] = consumer;
        logger.debug({consumer: consumer}, 'lib/consumerManager : consumer added');

        if (!!callback) {
            callback(null, consumer);
        }
    },

    createConsumerInstance = function (consumer, topic) {
        if (consumer.lock) {
            logger.debug('lib/consumerManager : consumer is locked, return as there is already an instance or its being recreated');
            return;
        }

        logger.debug('lib/consumerManager : creating a kafka client');
        var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http);
        logger.debug('lib/consumerManager : creating a new consumer');
        consumer.instance = createConsumer(consumer.group, topic, config.kafka.autocommit.http);

        logger.debug('lib/consumerManager : consumer created');
        consumer.instance.on('message', function (m) {
            logger.trace({messages: consumer.messages, message: m}, 'lib/consumerManager : consumer message received');
            consumer.messages.push(m);
            consumer.byteSize += Buffer.byteLength(m.value);

            if (consumer.byteSize > (16 * 1024 * 1024)) {
                logger.info('lib/consumerManager : consumer message queue greater than acceptable size, pausing consumer');
                consumer.instance.pause();
            }
        });

        consumer.instance.on('rebalancing', function(event){
            logger.info('lib/consumerManager : Consumer rebalancing');
            consumer.messages = [];
        });

        consumer.instance.on('rebalanced', function () {
            logger.info({consumer : consumer.id, topicsPayloads: consumer.instance.topicPayloads},'lib/consumerManager : rebalanced consumer');
        });

        consumer.instance.on('error', function (e) {
            logger.error({ error: e, consumer: consumer.id }, 'lib/consumerManager : Error in consumer instance. Closing and recreating...');
            if (!consumer.lock) {
                consumer.lock = true;
                logger.debug({ consumer: consumer.id }, 'lock enabled on consumer');
                consumer.instance.close(false, function () {
                    setTimeout(function () {
                        logger.info({consumer: consumer.id}, 'lib/consumerManager : Recreating consumer');
                        createConsumerInstance(consumer, topic);
                        consumer.lock = false;
                        logger.debug({ consumer: consumer.id }, 'lock released on consumer');
                    }, 1000);
                });
            }
        });

        consumer.instance.on('offsetOutOfRange', function (e) {
            logger.warn({ error: e }, 'lib/consumerManager : Received alert for offset out of range.');
        });
    },

    findOffsetInMap = function(map, searchItem) {
        logger.trace({map : map}, 'searching the offset map');
        return map.find(function(item) {
            return  item.topic === searchItem.topic &&
                    item.partition === searchItem.partition;
        });
    },

    createOffsetItem = function(message) {
        // note : metadata is an arbitrary value for kafka-node clients
        return {
            topic : message.topic || '',
            partition: message.partition || 0,
            offset : message.offset || 0,
            metadata : 'm'
        };
    },

    createOffsetMap = function(messages) {
        return messages.reduce(function(map, message){
            logger.trace(message, 'creating offset map for message');
            var offsetItem = findOffsetInMap(map, message);

            logger.trace({offsetItem: offsetItem || 'nothing'}, 'offset item from map');
            if (offsetItem === undefined) {
                map.push(createOffsetItem(message));
            } else {
                if (message.offset > offsetItem.offset) {
                    offsetItem.offset = message.offset;
                }
            }
            return map;
        }, []);
    },

    getMessages = function (consumer, callback) {

        consumer.lastPoll = Date.now();

        logger.trace({buffer: consumer.messages}, 'consumer message buffer');
        var messages = consumer.messages.splice(0, consumer.messages.length);
        consumer.offsetMap = createOffsetMap(messages);

        if (messages.length === 0) {
            return callback(null, []);
        }

        if (consumer.autoCommitEnable) {
            logger.trace({ consumer: consumer.id }, 'lib/consumerManager : Autocommit.');
            consumer.instance.commit(true);
        }

        if (consumer.instance.paused) {
            consumer.instance.resume();
        }

        logger.trace({ consumer: consumer.id }, 'lib/consumerManager : returning ' + messages.length + ' messages.');
        callback(null, messages.map(function (m) {
            return {
                topic: m.topic,
                partition: m.partition,
                offset: m.offset,
                key: m.key.toString(),
                value: m.value
            };
        }));
    },

    commitOffsets = function(consumer, cb) {
        consumer.instance.client.sendOffsetCommitRequest(
            consumer.groupid,
            consumer.offsetMap.map(function(item){
                item.fetchMaxBytes = consumer.options.fetchMaxBytes;
                return item;
            }),
            callback);
    },

    deleteConsumer = function (consumer, cb) {
        logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Removing consumer from set.');
        delete consumers[consumer.id];
        logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Closing consumer...');
        if (!!consumer.instance) {
            consumer.instance.close(false, function () {
                logger.debug({ consumer: consumer.id }, 'lib/consumerManager : Consumer closed.');
                if (!!cb) {
                    cb();
                }
            });
        }

        if (!!cb) {
            cb();
        }
        return;
    },

    timeoutConsumers = function () {
        logger.debug('lib/consumerManager : Looking for timed-out consumers.');
        var timeoutTime = Date.now() - config.consumer.timeoutMs;
        for (var i in consumers) {
            var consumer = consumers[i];
            if (consumer.instance && consumer.lastPoll < timeoutTime) {
                logger.debug({ consumer : consumer.id }, 'lib/consumerManager : Consumer timed-out.');
                deleteConsumer(consumer);
            }
        }
        logger.debug('lib/consumerManager : Done looking for timed-out consumers.');
    };

module.exports = {
    get: get,
    add: add,
    createInstance: createConsumerInstance,
    getMessages: getMessages,
    commitOffsets: commitOffsets,
    delete: deleteConsumer,
    timeout: timeoutConsumers,
    create: createConsumer
};
