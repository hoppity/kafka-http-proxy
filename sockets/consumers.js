var logger = require('../logger.js').logger;
var uuid = require('uuid');

module.exports = function (server) {
    var io = require('socket.io')(server);

    logger.info('initialising socket.io');

    io.on('connection', function (socket) {
        socket.uuid = uuid.v4();
        logger.info({socket: socket.uuid}, 'sockets/consumer.connection');

        socket.heartbeat = setInterval(function () {
            logger.info({socket: socket.uuid}, 'sockets/consumer.heartbeat_send');
            socket.emit('heartbeat', socket.uuid);
        }, 5000);

        socket.on('heartbeat', function (data) {
            logger.info({data: data}, 'sockets/consumer.heartbeat_received');
        });
        socket.on('disconnect', function (data) {
            clearInterval(socket.heartbeat);    
            logger.info({socket:socket.uuid}, 'sockets/consumer.disconnect');
        });
    });
};