var kafka = require('kafka-node'),
    murmur = require('murmurhash-js'),
    config = require('../config'),
    topics = require('../lib/topics.js'),

    client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0,
    seed = config.kafka.producerSeed,

    log = require('../logger.js'),
    logger = log.logger,

    base64Regex = new RegExp("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

module.exports = function (app) {

    app.put('/topics/:topic', function (req, res) {
        var topic = req.params.topic;

        logger.debug({ topic: topic }, 'Creating topic.');

        producer.createTopics([topic],
            false,
            function (err, data) {
                if (err) {
                    logger.error({error: err, topic: topic}, 'error creating topic');
                    return res.status(500).json({ error: err, message: 'Error creating topic.' });
                }
                res.json({ message: data });
            });
    });

    app.post('/topics/:topic', function (req, res) {
        if (!req.body || !req.body.records) {
            return res.status(500).json({error : "The Records field is required"}).send();
        }

        logger.trace('posting information to topic');

        var topic = req.params.topic;
        logger.trace({topic: topic}, 'posting information to topic');


        topics.partitions(topic, function (err, data) {
            logger.trace('loading partition data in topic controller');
            if (err) {
                logger.error({error: err}, 'error getting partitions');
                return res.status(500).json({ error: err }, 'error getting partitions');
            }

            logger.trace({data: req.body}, 'message body data');

            var numPartitions = data;
            var payloads = [];

            req.body.records.forEach(function (p) {
                // ensure that the p.value is a string, else it will cause an kafka error
                if (!p.value) {
                    p.value = '';
                } else if (typeof p.value !== 'string') {
                    p.value = JSON.parse(p.value);
                } else if (base64Regex.test(p.value)) {
                    p.value = new Buffer(p.value, 'base64').toString();
                }

                var hasKey = p.key !== null && typeof p.key !== 'undefined',
                    hasPartition = p.partition !== null && typeof p.partition !== 'undefined',
                    result = {
                        topic: topic,
                        messages: [ hasKey ? new kafka.KeyedMessage(p.key, p.value) : p.value ]
                    };
                if (hasKey) {
                    result.partition = murmur.murmur2(p.key, seed) % numPartitions;
                }
                else if (hasPartition) {
                    result.partition = p.partition;
                }

                var existing = payloads.filter(function (m) {
                    return m.partition == result.partition;
                });

                if (existing && existing.length) {
                    return existing[0].messages.push(result.messages[0]);
                }
                payloads.push(result);
            });

            logger.trace({data: data, messages: payloads}, 'ready to send the data');
            producer.send(payloads, function (err, data) {
                logger.trace({data: data}, 'data sent');
                if (err) {
                    logger.error({error: err}, 'error producing messages');
                    return res.status(500).json({error: err});
                }

                var topicResult = data[topic];
                var results = [];
                for (var i in topicResult) {
                    results.push({ partition: i, offset: topicResult[i] });
                }
                logger.trace({results: results.length}, 'controllers/topics : produced messages');
                res.json({ offsets: results });
            });
        });

    });

};
