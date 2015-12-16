var kafka       = require('kafka-node'),
    uuid        = require('uuid'),
    offsets     = require('../lib/offsets.js'),
    config      = require('../config'),
    consumerManager = require('../lib/consumerManager'),
    topics      = require('../lib/topics'),
    log         = require('../logger.js'),
    logger      = log.logger,


    getConsumerId = function (group, instanceId) {
        return group + '/' + instanceId;
    },
    getConsumer = function (group, instanceId) {
        return consumerManager.get(group, instanceId);
    },

    createConsumerInstance = function (consumer, topic) {
        return consumerManager.createInstance(consumer, topic);
    },

    consumerTimeoutMs = config.consumer.timoutMs,

    deleteConsumer = function (consumer, cb) {
        return consumerManager.delete(consumer, cb);
    },

    getMessages = function (consumer) {
        return consumerManager.getMessages(consumer);
    };

module.exports = function (app) {

    setInterval(consumerManager.timeout, 10000);

    app.post('/consumers/:group', function (req, res) {

        var group = req.params.group;

        var consumer = {
            group: group,
            autoOffsetReset: req.body['auto.offset.reset'],
            autoCommitEnable: req.body['auto.commit.enable']
        };
        logger.debug(consumer, 'controllers/consumers : New consumer.');
        try {
            consumerManager.add(consumer);
            res.json({
                instance_id: consumer.instanceId,
                base_uri: req.protocol + '://' + req.hostname + ':' + config.port + req.path + '/instances/' + consumer.instanceId
            });
        }
        catch(err)
        {
            logger.error({error: err}, 'unable to add consumer');
            res.status(500).json({error: err});
        }
    });

    app.get('/consumers/:group/instances/:id/topics/:topic', function (req, res) {
        logger.trace({params: req.params}, 'controllers/consumers : getting consumer');
        var consumer = getConsumer(req.params.group, req.params.id);
        var topic = req.params.topic;

        if (!consumer) {
            return res.status(404).json({ error: 'controllers/consumers : Consumer not found.' });
        }

        if (consumer.topics.indexOf(topic) == -1) {

            topics.exists(topic, function (err, data) {
                if (err) {
                    return res.json({ error: 'Could not find topic ' + topic });
                }
                if (!consumer.instance) {
                    logger.debug('controllers/consumers : no consumer instance, creating');
                    createConsumerInstance(consumer, topic);
                    logger.debug('controllers/consumers : consumer instance created');
                }
                else {
                    //TODO: support adding topics
                }
                logger.debug('controllers/consumers : add topic to consumer topic list');
                consumer.topics.push(req.params.topic);

                setTimeout(function () {
                    res.json( getMessages(consumer) );
                }, 1000);
            });

        }
        else {
            res.json( getMessages(consumer) );
        }
    });

    app.post('/consumers/:group/instances/:id/offsets', function (req, res) {
        logger.trace('request received, commit offsets');
        var consumer = getConsumer(req.params.group, req.params.id);

        if (!consumer) {
            return res.status(404).json({ message: 'Consumer not found.' });
        }

        logger.trace({consumer : consumer.offsetMap}, 'consumer data for offset commit');
        offsets.commitOffsets(consumer.group, consumer.offsetMap, function(err, data) {
            logger.trace('offsets commited');
            if (err) {
                return res.status(500).json({ 'error': err });
            }

            logger.trace('sending offset response to client');
            return res.json([]);
        });
    });

    app.delete('/consumers/:group/instances/:id', function (req, res) {

        var consumer = getConsumer(req.params.group, req.params.id);

        if (!consumer) {
            return res.status(404).json({ 'message': 'Consumer not found.' });
        }

        deleteConsumer(consumer, function () { res.json({}); });
    });

};
