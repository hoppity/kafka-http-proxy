var kafka = require('kafka-node'),
    uuid = require('uuid'),
    config = require('../config'),
    consumerManager = require('../lib/consumerManager'),
    consumers = new Object(),
    compression = config.kafka.compression || 0,
    logger = require('../logger'),

    getConsumerId = function (group, instanceId) {
        return group + '/' + instanceId;
    },
    getConsumer = function (group, instanceId) {
        return consumerManager.get(group, instanceId);
    },

    createConsumerInstance = function (consumer, topic) {
        consumerManager.createInstance(consumer, topic);
    },

    consumerTimeoutMs = config.consumer.timoutMs,

    deleteConsumer = function (consumer, cb) {
        consumerManager.delete(consumer, cb)
    },
    timeoutConsumers = function () {
        var timeoutTime = Date.now() - consumerTimeoutMs;
        for (var i in consumers) {
            var consumer = consumers[i];
            if (consumer.lastPoll < timeoutTime) {
                deleteConsumer(consumer);
            }
        }
    };

module.exports = function (app) {

    setInterval(timeoutConsumers, 10000);

    app.post('/consumers/:group', function (req, res) {

        var group = req.params.group;

        var consumer = {
            group: group,
            autoOffsetReset: req.body['auto.offset.reset'],
            autoCommitEnable: req.body['auto.commit.enable']
        };
        consumerManager.add(consumer);

        logger.debug(consumer, 'New consumer.');

        res.json({
            instance_id: consumer.instanceId,
            base_uri: req.protocol + '://' + req.hostname + ':' + config.port + req.path + '/instances/' + consumer.instanceId
        });

    });

    app.get('/consumers/:group/instances/:id/topics/:topic', function (req, res) {
        var consumer = getConsumer(req.params.group, req.params.id),
            topic = req.params.topic;

        if (!consumer) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        if (consumer.topics.indexOf(topic) == -1) {

            if (!consumer.instance) {
                createConsumerInstance(consumer, topic);
            }
            else {
                //TODO: support adding topics
            }
            consumer.topics.push(req.params.topic);

            setTimeout(function () {
                res.json([]);
            }, 1000);
        }
        else {
            var messages = consumer.messages.splice(0, consumer.messages.length);
            if (messages.length == 0) {
                return res.json([]);
            }
            res.json(messages.map(function (m) {
                return {
                    topic: m.topic,
                    partition: m.partition,
                    offset: m.offset,
                    key: m.key.toString(),
                    value: m.value
                };
            }));
            if (consumer.autoCommitEnable) {
                logger.debug({ consumer: consumer.id }, 'Autocommit.');
                consumer.instance.commit(true);
            }
            consumer.lastPoll = Date.now();
        }
    });

    app.post('/consumers/:group/instances/:id/offsets', function (req, res) {
        var consumer = getConsumer(req.params.group, req.params.id);

        if (!consumer) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        consumer.instance.commit(true, function (e, data) {
            if (e) {
                return res.status(500).json({ error: e });
            }
            return res.json([]);
        });
    });

    app.delete('/consumers/:group/instances/:id', function (req, res) {
        
        var consumer = getConsumer(req.params.group, req.params.id);

        if (!consumer) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        deleteConsumer(consumer, function () { res.json({}); });
    });

};