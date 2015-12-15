var kafka       = require('kafka-node');
var config      = require('./config.js');
var logger      = require('../logger.js');

var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId);
var offset = new kafka.Offset(client);

var commitOffsets = function(groupId, offsetMap, callback) {
    offset.commit(groupid, offsetMap, callback);
};


module.exports = {
    commitOffsets : commitOffsets
};
