var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;
var producers   = require('../lib/producers.js');
var socketIO    = require('socket.io');
var uuid        = require('uuid');

module.exports = function (server) {
    var io = socketIO(server, { path: '/sockets/producer' });

    logger.info('initialising producer socket on /sockets/producer');

    io.on('connection', function (socket) {
        socket.uuid = uuid.v4();
        logger.debug({ id: socket.uuid }, 'new producer connected');
        socket.producer = producers.create();

        var errorHandler = function (e) {
            logger.error({id: socket.uuid, error: e}, 'error in producer');
            if (typeof e === 'string') {
                e = { error: e, message: 'error in producer' }
            }
            socket.emit('error', e);
        };

        socket.producer.on('ready', function () {
            logger.debug({id: socket.uuid}, 'producer ready');
            socket.emit('ready');
        });

        socket.producer.on('error', function(e){
            errorHandler({ error: e, message: 'socket error occured' });
        });

        socket.on('disconnect', function () {
            logger.debug({id: socket.uuid}, 'producer disconnected');
            if (!!socket.producer) {
                socket.producer.close();
                delete socket.producer;
            }
        });

        socket.on('createTopic', function (data, callback) {
            var topic = data;
            logger.info({id: socket.uuid, topic: topic}, 'Creating topic...');
            
            if (!socket.producer.ready) {
                errorHandler({ error: 'producer not ready', message: 'failed to create topic' });
                callback('Failed to create topic - producer not ready.');
            }

            socket.producer.createTopics(
                [topic],
                false,
                function (err, data) {
                    if (err) {
                        logger.error({id: socket.uuid, error: err, topic: topic}, 'error creating topic');
                        //socket.emit('error', {error: err, message: 'Error creating topic.'});
                        callback({ error: err, message: 'failed to create topic' });
                        return;
                    }
                    logger.trace({id: socket.uuid, topic: topic}, 'topic created');
                    callback(err, data);
                });
        });

        socket.on('publish', function(data, callback) {
            logger.trace({id: socket.uuid, message: data}, 'publishing message(s)');

            if (typeof data === 'string') {
                data = JSON.parse(data);
            }

            if (!socket.producer.ready) {
                errorHandler({ error: 'producer not ready', message: 'failed to publish' });
                callback('Failed to publish - producer not ready.');
            }
            logger.trace({id: socket.uuid, message: data}, 'producer ready, sending the message');

            producers.publish(socket.producer, data, function(err, response) {
                logger.trace({id: socket.uuid, err: err, response: response}, 'message(s) published');

                if (err) {
                    errorHandler({ error: err, message: 'failed to publish' });
                }

                callback(err, response);
            });
        });

    });

};