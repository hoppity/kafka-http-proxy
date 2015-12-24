var kafka       = require('kafka-node'),
    uuid        = require('uuid'),
    offsets     = require('../lib/offsets.js'),
    config      = require('../config'),
    consumers   = require('../lib/consumers.js'),
    topics      = require('../lib/topics.js'),
    logger      = require('../logger.js').logger,

    getConsumerId = function (group, instanceId) {
        return group + '/' + instanceId;
    },
    getConsumer = function (group, instanceId, callback) {
        return consumers.get(group, instanceId, callback);
    },

    createConsumerInstance = function (consumer, topic) {
        return consumers.createInstance(consumer, topic);
    },

    deleteConsumer = function (consumer, callback) {
        return consumers.delete(consumer, callback);
    },

    getMessages = function (consumer, callback) {
        return consumers.getMessages(consumer, callback);
    };


module.exports = function (app) {

    setInterval(consumers.timeout, 10000);

    app.post('/consumers/:group', function (req, res) {

        var group = req.params.group;

        var consumer = {
            group: group,
            autoOffsetReset: req.body['auto.offset.reset'],
            autoCommitEnable: req.body['auto.commit.enable']
        };
        logger.trace(consumer, 'controllers/consumers : New consumer.');
            consumers.add(consumer, function(err, data){
                if (err) {
                    logger.error({error: err}, 'unable to add consumer');
                    return res.status(500).json({error: err});
                }

                return res.json({
                    instance_id: consumer.instanceId,
                    base_uri: req.protocol + '://' + req.hostname + ':' + config.port + req.path + '/instances/' + consumer.instanceId
                });
            });
    });

    app.get('/consumers/:group/instances/:id/topics/:topic', function (req, res) {
        logger.trace({url: req.originalUrl}, 'external request to get information');
        function retrieveMessages(consumer, retry) {
            logger.trace({consumer : consumer.id}, 'retrieving messages');
            return getMessages(consumer, function (err, messages){
                if (err) {
                    return res.status(500).json({
                        error: err,
                        message: 'unable to retrieve messages',
                        url: req.originalUrl
                    });
                }

                if ((!messages || messages.length === 0) && retry === true){
                    return setTimeout(function() {
                        return retrieveMessages(consumer, false);
                    }, 100);
                }

                logger.trace({url: req.originalUrl, messages: messages}, 'sending back messages');
                return res.json(messages);
            });

        }

        logger.trace({params: req.params}, 'controllers/consumers : getting consumer');
        getConsumer(req.params.group, req.params.id, function(err, consumer){
            var topic = req.params.topic;

            if (!consumer || err) {
                return res.status(404).json({ msg: 'Consumer not found.', error: err });
            }

            if (consumer.topics.indexOf(topic) == -1) {

                topics.exists(topic, function (err, data) {
                    if (err) {
                        return res.json({ error: 'Could not find topic ' + topic });
                    }
                    if (!consumer.instance) {
                        logger.trace('controllers/consumers : no consumer instance, creating');
                        createConsumerInstance(consumer, topic);
                        logger.trace('controllers/consumers : consumer instance created');
                    }
                    else {
                        //TODO: support adding topics
                    }
                    logger.trace('controllers/consumers : add topic to consumer topic list');
                    consumer.topics.push(req.params.topic);

                    setTimeout(function () {
                        retrieveMessages(consumer, true);
                    }, 1000);
                });

            }
            else {
                retrieveMessages(consumer, true);
            }
        });
    });

    app.post('/consumers/:group/instances/:id/offsets', function (req, res) {
        logger.trace('request received, commit offsets');
        getConsumer(req.params.group, req.params.id, function (err, consumer){
            if (err) {
                return res.status(404).json({ error: err });
            }

            logger.trace({consumer : consumer.offsetMap}, 'consumer data for offset commit');
            if (consumer.offsetMap.length === 0) {
                return res.json([]);
            }

            offsets.commitOffsets(consumer.group, consumer.offsetMap, function(err, data) {
                logger.trace('offsets commited');
                if (err) {
                    return res.status(500).json({ 'error': err });
                }

                logger.trace('sending offset response to client');
                return res.json([]);
            });
        });
    });

    app.delete('/consumers/:group/instances/:id', function (req, res) {

        getConsumer(req.params.group, req.params.id, function (err, consumer){
            if (!consumer || err) {
                return res.status(404).json({ message: 'Consumer not found.', error: err });
            }

            deleteConsumer(consumer, function () {
                return res.json({});
            });
        });

    });

};
