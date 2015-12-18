var socket = require('socket.io-client')('http://localhost:8085');
var Promise = require('promise');

socket.on('connect', function () {
    console.log('connect');
    socket.emit('subscribe', { group: 'test_group', topic: 'test_topic' });
});

socket.on('message', function (data) {
    console.log('Received - ' + (Date.now() - data.value) + 'ms');
});

socket.on('error', function (data) {
    console.log('error - ' + JSON.stringify(data));
});

socket.on('disconnect', function () {
    console.log('disconnect');
});

return new Promise(function (res, rej) {
    process.on('SIGINT', function() {
        if (socket) socket.close();
        res();
    });
});
