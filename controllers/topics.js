var kafka = require('kafka-node'),
    murmur = require('murmurhash-js'),
    config = require('../config'),
    client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0,
    seed = 0x9747b28c,

    logger = require('../logger.js'),

    refreshTopic = function (topic, cb) {
        client.topicExists([topic], function (err, data) {
            if (err) {
                logger.error({error: err, request: req, response: res});
                return cb(err);
            }
            client.refreshMetadata([topic],  function (err, data) {
                if (err) return cb(err);
                cb();
            });
        });
    };

module.exports = function (app) {

    app.put('/topics/:topic', function (req, res) {

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

        var topic = req.params.topic;

        refreshTopic(topic, function (err) {
            if (err) {
                logger.error({error: err, request: req, response: res});
                return res.status(500).json({ error: err });
            }

            var numPartitions = client.topicPartitions[topic].length,
                messages = req.body.records.map(function (p) {
                    var hasKey = p.key !== null && typeof p.key !== undefined,
                        hasPartition = p.partition !== null && typeof p.partition !== undefined,
                        result = {
                            topic: topic,
                            messages: hasKey ? new kafka.KeyedMessage(p.key, p.value) : p.value,
                        };
                    if (hasKey) {
                        result.partition = murmur.murmur2(p.key, seed) % numPartitions;
                    }
                    else if (hasPartition) {
                        result.partition = p.partition
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