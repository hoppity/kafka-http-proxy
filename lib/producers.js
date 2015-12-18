var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;

var createProducer = function() {
    logger.trace({zk: config.kafka.zkConnect}, 'creating the new producer instance');
    var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.socket);
    return new kafka.HighLevelProducer(client);
};

var publish = function(producer, payloads, cb) {
    producer.send(payloads, function(err, data){
        logger.trace({errors: err}, 'completed publishing data');

        if (cb) {
            cb(err, data);
        }
    });
};

module.exports = {
    create: createProducer,
    publish: publish
};
