var socket = require('socket.io-client')('http://localhost:8085');
var Promise = require('promise');
var stop = false;
socket.on('connect', function () {
    console.log('connect');

});

socket.on('error', function (data) {
    console.log('error - ' + JSON.stringify(data));
});

socket.on('disconnect', function () {
    console.log('disconnect');
});

var messagePump = function() {
    setTimeout(function(){
        if (!stop) {
            var date = Date.now();
            socket.emit('publish', [{topic: 'test_topic', messages: date}]);
            messagePump();
        }
    }, 2);
};

messagePump();

return new Promise(function (res, rej) {
    process.on('SIGINT', function() {
        stop = true;
        if (socket) socket.close();
        res();
    });
});
