var kafka = require('kafka-node'),
    config = require('config-node')(),
    client = new kafka.Client(config.kafka.zkConnect, 'kafka-rest-proxy'),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0;

module.exports = function (app) {

    app.put('/topics/:topic', function (req, res) {

        producer.createTopics([req.params.topic],
            false,
            function (err, data) {
                if (err) return res.status(500).json({ error: err });
                res.json({ message: data });
            });
    });

    app.post('/topics/:topic', function (req, res) {

        var messages = req.body.payload.map(function (p) {
                return typeof(p.key) !== 'undefined'
                    ? new kafka.KeyedMessage(p.key, p.value)
                    : p.value;
            });

        producer.send([{
            topic: req.params.topic,
            messages: messages
        }], function (err, data) {
            if (err) return res.status(500).json({error: err});
            res.json(data);
        });

    })

};