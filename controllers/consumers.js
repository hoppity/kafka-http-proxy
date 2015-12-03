var kafka = require('kafka-node'),
    uuid = require('uuid'),
    config = require('config-node')(),
    client = new kafka.Client(config.kafka.zkConnect, 'kafka-rest-proxy'),
    consumers = [],
    compression = config.kafka.compression || 0;

module.exports = function (app) {

    app.post('/consumers/:name', function (req, res) {

        var autoOffsetReset = 
            typeof (req.body['auto.offset.reset']) === 'undefined'
            ? 'largest' : req.body['auto.offset.reset'];
        var autoCommitEnable =
            typeof (req.body['auto.commit.enable']) === 'undefined'
            ? true : req.body['auto.commit.enable'];

        var consumer = {
            group: req.params.group,
            id: uuid.v4(),
            autoOffsetReset: autoOffsetReset,
            autoCommitEnable: autoCommitEnable,
            instance: undefined,
            topics: [],
            messages: []
        };
        consumers.push(consumer);

        res.json({
            instance_id: consumer.id,
            base_uri: req.protocol + '://' + req.hostname + ':' + config.port + req.path + '/instances/' + consumer.id
        });

    });

    app.get('/consumers/:name/instances/:id/topics/:topic', function (req, res) {

        var matches = consumers.filter(function (c) {
            return c.group == req.params.group
                && c.id == req.params.id;
        });

        if (matches.length == 0) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        var consumer = matches[0],
            topic = req.params.topic;

        if (consumer.topics.indexOf(topic) == -1) {

            if (!consumer.instance) {
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
                    console.error(m);
                });
                consumer.instance.on('offsetOutOfRange', function (e) {
                    console.warn(m);
                });

            }
            else {
                //TODO: support adding topics
            }
            consumer.topics.push(req.params.topic);
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
            consumer.instance.commit();
        }
    });

    app.delete('/consumers/:name/instances/:id', function (req, res) {
        
        var matches = consumers.filter(function (c) {
            return c.group == req.params.group
                && c.id == req.params.id
        });

        if (matches.length == 0) {
            return res.status(404).json({ error: 'Consumer not found.' });
        }

        consumers.splice(consumers.indexOf(matches[0]), 1);

        res.json({});
    });

};