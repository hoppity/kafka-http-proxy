var kafka = require('kafka-node'),
    config = require('../config'),
    getClient = function () {
        return new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
    };

module.exports = {
    exists: function (topic, cb) {
        getClient()
            .topicExists([topic], function (err, data) {
                cb(err, data);
            });
    }
};
