var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;

var createProducer = function() {
    logger.trace({zk: config.kafka.zkConnect}, 'creating the new producer instance');
    var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.socket);
    return new kafka.HighLevelProducer(client);
};

var publish = function(producer, payloads, cb) {
    var payloadsToPublish = [];
    payloads.forEach(function (p) {
        // ensure that the p.value is a string, else it will cause an kafka error
        if (!p.value) {
            p.value = '';
        } else if (typeof p.value !== 'string') {
            p.value = JSON.parse(p.value);
        }

        var hasKey = p.key !== null && typeof p.key !== 'undefined',
            hasPartition = p.partition !== null && typeof p.partition !== 'undefined',
            result = {
                topic: p.topic,
                messages: [ hasKey ? new kafka.KeyedMessage(p.key, p.value) : p.value ]
            };
        if (hasKey) {
            result.partition = murmur.murmur2(p.key, seed) % numPartitions;
        }
        else if (hasPartition) {
            result.partition = p.partition;
        }

        var existing = payloadsToPublish.find(function (m) {
            return m.topic === result.topic &&
                m.partition === result.partition;
        });

        if (existing) {
            return existing.messages.push(result.messages[0]);
        }
        payloadsToPublish.push(result);
    });

    logger.debug({payloads: payloadsToPublish}, 'transformed payloads. publishing...');
    producer.send(payloadsToPublish, function(err, data){
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
