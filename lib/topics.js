var kafka = require('kafka-node'),
    config = require('../config'),
    logger = require('../logger.js').logger,
    topicMap = [],

    client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http),
    getClient = function () {
        logger.trace('returning found client');
        return client;
    },

    exists = function (client, topic, callback) {
        client.topicExists([topic], function (err, data) {
            topicMap[topic] = {};
            callback(err, data);
        });
    },

    refresh = function (client, topic, callback) {
        logger.trace({topic: topic}, 'refreshing the topic information');
        exists(client, topic, function (err) {
            if (err) return callback(err);

            logger.trace({topic: topic}, 'refreshing the metadata');
            client.refreshMetadata([topic], callback);
        });
    },

    partitions = function (client, topic, callback) {
        logger.trace({ topic : topic}, 'loading partitions for topic');
        refresh(client, topic, function (err) {
            if (err) return callback(err);

            logger.trace({ topic : topic}, 'loading the topic partitions from kafka');
            callback(err, client.topicPartitions[topic].length);
        });
    };

module.exports = {
    exists: function (topic, callback) {
        if (!!client.topicPartitions[topic])
            return callback(undefined, true);
        return exists(getClient(), topic, callback);
    },
    refresh: function (topic, callback) {
        return refresh(getClient(), topic, callback);
    },
    partitions: function (topic, callback) {
        if (!!client.topicPartitions[topic])
            return callback(undefined, client.topicPartitions[topic].length);

        logger.trace('getting partitions');
        return partitions(getClient(), topic, callback);
    }
};
