var request = require('request-promise'),
    args = require('yargs').argv;

var log = require('../logger.js');
var logger = log.logger();

/*
 * args:
 * - baseUri
 * - topic
 * - key
 * - value
 * - partition
 */

var topicUri = args.baseUri + '/topics/' + args.topic,
    record = {};

args.partition = args.partition || 0;

if (args.key) record.key = args.key.toString();
if (args.value) record.value = args.value.toString();
if (args.partition) record.partition = args.partition.toString();

var options = {
    uri: topicUri,
    method: 'POST',
    json: {
        records: [ record ]
    },
    headers: {
        'Content-Type': 'application/vnd.kafka.v1+json'
    }
};
logger.debug(JSON.stringify(options));
return request.post(options)
    .then(function (r) {
        logger.info(r);
    })
    .catch(function (e) {
        logger.error(e);
    });
