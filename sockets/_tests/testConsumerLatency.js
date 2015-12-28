var socket = require('socket.io-client')('http://localhost:8085', {path: '/sockets/consumer'});
var request = require('request-promise');
var Promise = require('promise');

var topic = 'perf-test-' + Date.now();
var topicUri = 'http://localhost:8085/topics/' + topic;
var stop = false;
var post = function () {
    if (stop) {
        clearInterval(postInterval);
        postInterval = undefined;
    }

    var options = {
        uri: topicUri,
        method: 'POST',
        json: {
            records: [{ value: Date.now() }]
        },
        headers: {
            'Content-Type': 'application/vnd.kafka.v1+json'
        }
    };
    request.post(options);
};
var postInterval;

socket.on('connect', function () {
    console.log('connect');
    socket.emit('createTopic', topic, function (e,r) {
        if (e) {
            console.log(e);
            process.exit(1);
        }
        socket.emit('subscribe', { group: topic, topic: topic });
    })
});

socket.on('subscribed', function () {
    postInterval = setInterval(post, 100);
});

socket.on('message', function (data) {
    //console.log('message - ' + JSON.stringify(data));
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
        stop = true;
        var teardown = function () {
            if (!!postInterval) return setTimeout(teardown, 500);
            if (socket) socket.close();
            res();
        };
        setTimeout(teardown, 500);
    });
});
