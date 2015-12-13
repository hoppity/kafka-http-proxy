var kafka = require('kafka-node'),
    config = require('../config'),

    _client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId),
    getClient = function () {
        return _client;
    },

    exists = function (client, topic, cb) {
        client.topicExists([topic], function (err, data) {
            cb(err, data);
        });
    },
    refresh = function (client, topic, cb) {
        exists(client, topic, function (err) {
            if (err) return cb(err);

            client.refreshMetadata([topic], cb);
        });
    },
    partitions = function (client, topic, cb) {
        refresh(client, topic, function (err) {
            if (err) return cb(err);

            cb(err, client.topicPartitions[topic].length);
        });
    };

module.exports = {
    exists: function (topic, cb) {
        return exists(getClient(), topic, cb);
    },
    refresh: function (topic, cb) {
        return refresh(getClient(), topic, cb);
    },
    partitions: function (topic, cb) {
        return partitions(getClient(), topic, cb);
    }
};
