var request = require('request-promise'),
    Promise = require('promise'),
    now = Date.now(),
    baseUri = 'http://localhost:8085',
    topicUriSuffix = '/topics/test.' + now,
    topicUri = baseUri + topicUriSuffix,
    createConsumerUri = baseUri + '/consumers/test.' + now;

var consumerUri,
    consumerTopicUri;

request.put(topicUri)
    .then(function (r) {
        console.log('created topic ' + r);
        return request.post(createConsumerUri);
    })
    .then(function (r) {
        console.log('created consumer ' + r);
        consumerUri = JSON.parse(r).base_uri;
        consumerTopicUri = consumerUri + topicUriSuffix;
        return request.get(consumerTopicUri);
    })
    .then(function (r) {
        var stop = false;

        var posting = setInterval(function () {
            if (stop) clearInterval(posting);

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
        }, 100);

        var polling,
            poll = function () {
            request.get(consumerTopicUri)
                .then(function (r) {
                    if (!stop) polling = setTimeout(poll);

                    var time = Date.now();
                    var result = JSON.parse(r);
                    if (!result || result.length === 0) return;

                    var times = result.map(function (r) { return time - r.value; });
                    var max = Math.max(times);
                    var ave = times.reduce(function (a,b) { return a + b; }) / times.length;
                    var min = Math.min(times);
                    console.log('Received: ' + times.length + 'msgs, Ave: ' + ave + 'ms, Max: ' + max + 'ms, Min: ' + min + 'ms');
                });
        };
        polling = setTimeout(poll);

        return new Promise(function (res, rej) {
            process.on('SIGINT', function() {
                stop = true;
                res();
            });
        });
    })
    .catch(function (e) {
        console.error(e);
    })
    .done(function () {
        if (consumerUri) {
            request.del(consumerUri);
        }
    });
