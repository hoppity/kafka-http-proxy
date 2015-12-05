var kafka = require('kafka-node'),
    uuid = require('uuid'),
    config = require('config-node')(),
    consumers = {},
    compression = config.kafka.compression || 0,

    getConsumerId = function (group, instanceId) {
        return group + '/' + instanceId;
    },
    getConsumer = function (group, instanceId) {
        return consumers[getConsumerId(group, instanceId)];
    },

    createConsumerInstance = function (consumer, topic) {
        var client = new kafka.Client(config.kafka.zkConnect, 'kafka-rest-proxy');
        consumer.instance = new kafka.HighLevelConsumer(client, [{
            topic: topic
        }], {
            groupId: consumer.group,
            // Auto commit config 
            autoCommit: false,
            // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms 
            fetchMaxWaitMs: 100,
            // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte 
            fetchMinBytes: 1,
            // The maximum bytes to include in the message set for this partition. This helps bound the size of the response. 
            fetchMaxBytes: 4 * 1024 * 1024, // 4MB
            // If set true, consumer will fetch message from the given offset in the payloads 
            fromOffset: false,
            // If set to 'buffer', values will be returned as raw buffer objects. 
            encoding: 'utf8'
        });
        consumer.instance.on('message', function (m) {
            consumer.messages.push(m);
        });
        consumer.instance.on('error', function (e) {
            console.error(e);
            consumer.instance.close(false, function () {
                setTimeout(function () {
                    console.log('recreating consumer');
                    createConsumerInstance(consumer, topic);
                }, 1000);
            });
        });
        consumer.instance.on('offsetOutOfRange', function (e) {
            console.warn(e);
        });
    };

module.exports = function (app) {

    app.post('/consumers/:group', function (req, res) {

        var group = req.params.group,
            instanceId = uuid.v4(),
            id = getConsumerId(group, instanceId);

        var autoOffsetReset = 
            typeof (req.body['auto.offset.reset']) === 'undefined'
            ? 'largest' : req.body['auto.offset.reset'];
        var autoCommitEnable =
            typeof (req.body['auto.commit.enable']) === 'undefined'
            ? true : req.body['auto.commit.enable'];

        var consumer = {
            group: group,
            id: instanceId,
            autoOffsetReset: autoOffsetReset,
            autoCommitEnable: autoCommitEnable,
            instance: undefined,
            topics: [],
            messages: [],
            created: new Date(),
            lastPoll: Date.now()
        };
        consumers[id] = consumer;

        res.json({
            instance_id: consumer.instanceId,
            base_uri: req.protocol + '://' + req.hostname + ':' + config.port + req.path + '/instances/' + consumer.id
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
            res.json( messages.map(function (m) {
                return {
                    topic: m.topic,
                    partition: m.partition,
                    offset: m.offset,
                    key: m.key.toString(),
                    value: m.value
                };
            }) );
            if (consumer.instance.autoCommitEnable) consumer.instance.commit(true);
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
        
        var id = getConsumerId(req.params.group, req.params.id),
            consumer = consumers[id];

        if (!consumer) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        consumer.instance.close(false, function () {
            delete consumers[id];

            res.json({});
        });
    });

};