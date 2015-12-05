var request = require('request-promise'),
    args = require('yargs').argv;

var topicUri = args.baseUri + '/topics/' + args.topic,
    record = {};

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
console.log(JSON.stringify(options));
return request.post(options)
    .then(function (r) {
        console.log(r);
    })
    .catch(function (e) {
        console.error(e);
    });
