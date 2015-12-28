var socket  = require('socket.io-client')('http://localhost:8085', { path: '/sockets/producer' });
var Promise = require('promise');

var publishInterval;
var publish = function () {
    var date = Date.now();
    socket.emit('publish', [{ topic: 'test.producer', value: date }], function (e, r) {
        if (e) {
            clearInterval(publishInterval);
            socket.close();
        }
        console.log((Date.now() - date) + 'ms')
    });
};

socket.on('connect', function () {
    console.log('connect');
});

socket.on('ready', function () {
    console.log('ready');

    socket.emit('createTopic', 'test.producer', function (e, r) {
        if (e) return console.log('error - ' + e);

        publishInterval = setInterval(publish, 100);
    });
});

socket.on('error', function (error) {
    console.error(typeof error === 'string' ? error : JSON.stringify(error));
    socket.close();
});