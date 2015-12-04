var kafka = require('kafka-node'),
    murmur = require('murmurhash-js'),
    config = require('config-node')(),
    client = new kafka.Client(config.kafka.zkConnect, 'kafka-rest-proxy'),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0,
    seed = 0x9747b28c,

    logger = require('../logger.js'),

    refreshTopic = function (topic, cb) {
        client.topicExists([topic], function (err, data) {
            if (err) return cb(err);
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
                    //logger.error({error: err, request: req, response: res});
                    return res.status(500).json({ error: err });
                }
                res.json({ message: data });
            });
    });

    app.post('/topics/:topic', function (req, res) {
        var topic = req.params.topic;

        refreshTopic(topic, function (err) {
            if (err) {
                //logger.error({error: err, request: req, response: res});
                return res.status(500).json({ error: err });
            }

            var numPartitions = client.topicPartitions[topic].length,
                messages = req.body.payload.map(function (p) {
                    return {
                        topic: topic,
                        messages: typeof(p.key) !== 'undefined'
                            ? new kafka.KeyedMessage(p.key, p.value)
                            : p.value,
                        partition: typeof(p.key) !== 'undefined'
                            ? murmur.murmur2(p.key, seed) % numPartitions
                            : undefined
                    };
                });

            producer.send(messages, function (err, data) {
                if (err) {
                    //logger.error({error: err, request: req, response: res});
                    return res.status(500).json({error: err});
                }
                res.json(data);
            });
        });

    })

};