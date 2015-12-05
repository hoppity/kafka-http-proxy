var request = require('request-promise'),
    args = require('yargs').argv;

var createConsumerUri = args.baseUri + '/consumers/' + args.consumerGroup,
    topicUriSuffix = '/topics/' + args.topic;

var consumerBaseUri,
    consumerTopicUri;

return request.post(createConsumerUri)
    .then(function (r) {
        console.log(r);
        var result = JSON.parse(r);
        var stop = false;
        consumerBaseUri = result.base_uri;
        consumerTopicUri = result.base_uri + topicUriSuffix;
        var polling,
            retries = 0,
            poll = function () {
                request.get(consumerTopicUri)
                    .then(function (r) {
                        if (r != '[]') console.log(r);
                        if (!stop) setTimeout(poll, 50);
                    })
                    .catch(function (e) {
                        console.error(e);
                        retries++;
                        if (retries <= 5 && !stop) setTimeout(poll, 50);
                    });
            };
        setTimeout(poll, 50);

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
        if (consumerBaseUri) return request.del(consumerBaseUri);
    });
