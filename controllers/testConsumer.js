var request = require('request-promise'),
    Promise = require('promise'),
    baseUri = 'http://localhost:8085',
    consumerUri;

request.post(baseUri + '/consumers/test.' + 1449182267952)
    .then(function (r) {
        console.log('created consumer ' + r);
        consumerUri = JSON.parse(r).base_uri;
        return request.get(consumerUri + '/topics/ducastest2');
    })
    .then(function (r) {
        return new Promise(function (res) {
            setTimeout(function() {
                res(r)
            }, 1000);
        });
    })
    .then(function (r) {
        console.log(r);
        var options = {
            uri: baseUri + '/topics/ducastest2',
            method: 'POST',
            json: {
                payload: [{ key: '123', value: '456' }]
            }
        };
        return request.post(options);
    })
    .then(function (r) {
        console.log(r);
        return new Promise(function (res) {
            setTimeout(function() {
                res(r)
            }, 1000);
        });
    })
    .then(function (r) {
        var max = 10,
            i = 0;
        return new Promise(function (res, rej) {
            var poll = function () {
                console.log('get');
                request.get(consumerUri + '/topics/ducastest2')
                    .then(function (r) {
                        var result = JSON.parse(r);
                        i++;
                        console.log(result);
                        if (result.length > 0)
                            res(result);
                        else if (i == max)
                            rej('ohnoes');
                        else
                            setTimeout(poll, 1000);
                    })
                    .catch(function (e) {
                        rej(e);
                    });
            };
            setTimeout(poll, 1000);
        });
    })
    .then(function (r) {
        console.log(r);
    })
    .catch(function (e) {
        console.error(e);
    })
    .done(function () {
        if (consumerUri) {
            request.del(consumerUri);
        }
    });
