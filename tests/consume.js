var request = require('request-promise'),
    args = require('yargs').argv;

var log = require('../logger.js');
var logger = log.logger;

/*
 * args:
 * - baseUri
 * - consumerGroup
 * - topic
 */

var createConsumerUri = args.baseUri + '/consumers/' + args.consumerGroup,
    topicUriSuffix = '/topics/' + args.topic;

var consumerBaseUri,
    consumerTopicUri;

return request.post(createConsumerUri)
    .then(function (r) {
        logger.info(r);
        var result = JSON.parse(r);
        var stop = false;
        consumerBaseUri = result.base_uri;
        consumerTopicUri = result.base_uri + topicUriSuffix;
        var polling,
            retries = 0,
            poll = function () {
                request.get(consumerTopicUri)
                    .then(function (r) {
                        if (r != '[]') logger.info(r, 'consumer test polling for info');
                        //if (!stop) setTimeout(poll, 50);
                    })
                    .catch(function (e) {
                        logger.error(e, 'consumer test trying again');
                        retries++;
                        //if (retries <= 5 && !stop) setTimeout(poll, 50);
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
        logger.error(e);
    })
    .done(function () {
        if (consumerBaseUri) return request.del(consumerBaseUri);
    });
