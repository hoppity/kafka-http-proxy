var kafka = require('kafka-node'),
    config = require('../config'),
    logger = require('../logger.js').logger,
    topicMap = [],

    _client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http),
    getClient = function () {
        logger.trace('returning found client');
        return _client;
    },

    exists = function (client, topic, cb) {
        client.topicExists([topic], function (err, data) {
            topicMap[topic] = {};

            cb(err, data);
        });
    },
    refresh = function (client, topic, cb) {
        logger.trace({topic: topic}, 'refreshing the topic information');
        exists(client, topic, function (err) {
            if (err) return cb(err);

            logger.trace({topic: topic}, 'refreshing the metadata');
            client.refreshMetadata([topic], cb);
        });
    },
    partitions = function (client, topic, cb) {
        logger.trace({ topic : topic}, 'loading partitions for topic');
        refresh(client, topic, function (err) {
            if (err) return cb(err);

            logger.trace({ topic : topic}, 'loading the topic partitions from kafka');
            cb(err, client.topicPartitions[topic].length);
        });
    };

module.exports = {
    exists: function (topic, cb) {
        if (!!_client.topicPartitions[topic])
            return cb(undefined, true);

        return exists(getClient(), topic, cb);
    },
    refresh: function (topic, cb) {
        return refresh(getClient(), topic, cb);
    },
    partitions: function (topic, cb) {
        if (!!_client.topicPartitions[topic])
            return cb(undefined, _client.topicPartitions[topic].length);

        logger.trace('getting partitions');
        return partitions(getClient(), topic, cb);
    }
};
