var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;

var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
var offset = new kafka.Offset(client);

var commitOffsets = function(groupId, offsetMap, callback) {
    logger.debug({offsetMap: offsetMap}, 'committing offsets');

    if (offsetMap === []) {
        logger.trace('no offsets found, returning');
        return callback(new Error('no offsets found to commit'), null);
    }

    logger.trace('committing the offsets to kafka');
    return offset.commit(groupId, offsetMap, callback);
};


module.exports = {
    commitOffsets : commitOffsets
};
