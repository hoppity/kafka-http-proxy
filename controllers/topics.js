var kafka = require('kafka-node'),
    murmur = require('murmurhash-js'),
    config = require('../config'),

    topics = require('../lib/topics.js'),

    client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0,
    seed = config.kakfa.producerSeed,

    log = require('../logger.js'),
    logger = log.logger;

module.exports = function (app) {

    app.put('/topics/:topic', function (req, res) {
        logger.debug('put information to topic');

        producer.createTopics([req.params.topic],
            false,
            function (err, data) {
                if (err) {
                    logger.error({error: err, request: req, response: res});
                    return res.status(500).json({ error: err });
                }
                res.json({ message: data });
            });
    });

    app.post('/topics/:topic', function (req, res) {
        logger.trace('posting information to topic');

        var topic = req.params.topic;

        topics.partitions(topic, function (err, data) {
            if (err) {
                logger.error({error: err, request: req, response: res});
                return res.status(500).json({ error: err });
            }

            var numPartitions = data,
                messages = req.body.records.map(function (p) {
                    var hasKey = p.key !== null && typeof p.key !== 'undefined',
                        hasPartition = p.partition !== null && typeof p.partition !== 'undefined',
                        result = {
                            topic: topic,
                            messages: hasKey ? new kafka.KeyedMessage(p.key, p.value) : p.value,
                        };
                    if (hasKey) {
                        result.partition = murmur.murmur2(p.key, seed) % numPartitions;
                    }
                    else if (hasPartition) {
                        result.partition = p.partition;
                    }
                    return result;
                });

            producer.send(messages, function (err, data) {
                if (err) {
                    logger.error({error: err, request: req, response: res});
                    return res.status(500).json({error: err});
                }

                var topicResult = data[topic];
                var results = [];
                for (var i in topicResult) {
                    results.push({ partition: i, offset: topicResult[i] });
                }
                res.json({ offsets: results });
            });
        });

    });

};
