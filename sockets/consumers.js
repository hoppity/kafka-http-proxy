var uuid        = require('uuid');
var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;
var consumers   = require('../lib/consumers.js');
var producers   = require('../lib/producers.js');
var _           = require('lodash');


var waitForClient = function(client, cb) {
    var retryCount = 0;

    var clientRetry = function(){
        setTimeout(function() {
            if (!client.ready && retryCount < 5) {
                logger.trace('client currently not ready, retrying');
                retryCount ++;
                clientRetry();
            } else {
                var err = !client.ready ? {message: 'client failed to initialise'} : null;
                if (!!cb) {
                    cb(err);
                }
            }
        }, 100);
    };

    if (!client.ready) {
        clientRetry();
    } else
    {
        if (!!cb) {
            cb();
        }
    }
};


var setupSubscriber = function(socket, data) {
    logger.info({data: data}, 'sockets/consumer : subscribe received');
    if (typeof data === 'string') {
        try {
            data = JSON.parse(data)
        } catch (e) {
            logger.warn({data: data, error: e}, 'sockets/consumer : could not parse json.');
            socket.emit('error', 'Could not parse JSON in subscribe: ' + e);
        }
    }

    var group = data.group;
    var topic = data.topic;

    if (!socket.consumer) {
        socket.consumer = consumers.create(group, topic, config.kafka.autocommit.socket);
    }

    waitForClient(socket.consumer, function(err) {
        logger.trace('subscriber client is now ready');
        if (err) {
            return logger.error({error: err}, 'failed to set up subscriber');
        }

        logger.debug({socket:socket.uuid, consumer:socket.consumer.id}, 'sockets/consumers : consumer created');
        socket.consumer.on('message', function (m) {
            logger.trace(m, 'sockets/consumers : message received');
            socket.emit('message', m);
        });

        socket.consumer.on('error', function (e) {
            logger.error({ error: e, consumer: socket.consumer.id }, 'sockets/consumer : Error in consumer instance. Closing and recreating...');
            //TODO: close and recreate
        });

        socket.consumer.on('offsetOutOfRange', function (e) {
            logger.warn({ error: e }, 'sockets/consumer : Received alert for offset out of range.');
        });
    });
};


var produceMessage = function(socket, data) {
    logger.trace({message: data}, 'sending message');

    if (typeof data === 'string') {
        data = JSON.parse(data);
    }

    if (!socket.producer.ready) {
        logger.trace('there are no producers ready, bailing');
        return socket.emit('produceMessageFailed', { message: 'producer failed to initialise', producerId: socket.producer.uuid});
    }
    logger.trace({message: data}, 'producer ready, sending the message');

    producers.publish(socket.producer, data, function(err, response){
        logger.trace({err: err, response: response}, 'message published');
        if (err) {
            return socket.emit('produceMessageFailed', { error: err, messageId: data.id, producerId: socket.producer.uuid});
        }

        return socket.emit('produceMessageComplete',{ messageId: data.id, producerId: socket.producer.uuid });
    });
};


var setupProducer = function(socket, cb) {
    if (!socket.producer) {
        socket.producer = producers.create();
        logger.trace({producer: socket.producer}, 'newly created producer');
        socket.producer.uuid = uuid.v4();
        logger.debug({id: socket.producer.uuid}, 'created a new publisher on the socket');
    }

    waitForClient(socket.producer, function(err){
        logger.trace('producer client wait is over');
        if (err) {
            return logger.error({error: err}, 'failed to set up producer');
        }

        socket.producer.on('error', function(e){
            logger.error({producerId: socket.producer.uuid, error: e}, 'socket producer had an error');
        });

        if (!!cb) {
            cb();
        }
    });
};

module.exports = function (server) {
    var io = require('socket.io')(server);

    logger.info('initialising socket.io');

    io.on('connection', function (socket) {
        socket.uuid = uuid.v4();
        socket.topics = [];

        logger.info({socket: socket.uuid}, 'sockets/consumer : connection established');

        socket.on('subscribe', function (data) {
            setupSubscriber(socket, data);
        });

        socket.on('createTopic', function (data) {
            logger.info({topic: data}, 'Creating topic...');
            var createTopic = function (topic) {
                var producer = socket.producer;

                if (!producer) {
                    return setupProducer(socket, function(){
                        logger.trace('producer setup complete, sending the message');
                        createTopic(topic);
                    });
                }

                if (!!producer && !producer.ready)
                    return setTimeout(function () { createTopic(topic); }, 100);
                
                producer.createTopics(
                    [topic],
                    false,
                    function (err, data) {
                        if (err) {
                            logger.error({error: err, topic: topic}, 'error creating topic');
                            socket.emit('error', {error: err, message: 'Error creating topic.'})
                            return;
                        }
                        logger.trace({topic: data}, 'topic created');
                        socket.emit('topicCreated', topic);
                    }
                );
            };
            createTopic(data);
        });

        var startingProducer = false;
        var messages = [];
        socket.on('publish', function(data) {
            if (!socket.producer) {
                setupProducer(socket, function(){
                    logger.trace('producer setup complete, sending the message');
                    messages.forEach(function(message) {
                        produceMessage(socket, message);
                    });
                    produceMessage(socket, data);
                    messages = [];
                });
            }
            else {
                if (!socket.producer.ready) {
                    // queue the messages
                    messages.push(data);
                    logger.trace({message: data}, 'hold message while producer starts');
                }
                else {
                    logger.trace({message: data}, 'instantly sending message');
                    produceMessage(socket, data);
                }
            }
        });

        socket.on('error', function(err){
            logger.error({error:err}, 'Socket level error on socket');
        });


        socket.on('disconnect', function (data) {
            if (socket.consumer) {
                socket.consumer.close(false);
            }

            logger.info({socket:socket.uuid}, 'sockets/consumer : disconnected');
        });
    });
};
