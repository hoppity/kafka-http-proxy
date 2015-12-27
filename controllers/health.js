var config = require('../config');
var kafka  = require('kafka-node');

var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);

module.exports = function (app) {

    app.get('/health/ping', function (req, res) {
        return res.send(config.health.okResponseText);
    });

    app.get('/health/connectivity', function (req, res) {
        var zkState = client.zk.state;

        return res.json({
            zk: {
                connectionString: config.kafka.zkConnect,
                state: client.zk.client.getState()
            },
            kafka: {
                brokers: client.brokerMetadata
            }
        });
    });
};