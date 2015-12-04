var bunyan = require('bunyan');
var config = require('config-node')();
var moment = require('moment');


var createStreams = function() {
    var streams = [
        {
            level: 'debug',
            stream : process.stdout
        }
    ];

    if (config.logging.type === 'file') {
        var filePath = config.logging.filePath.replace('##Date##', moment().format('YYYYMMDD'))
        streams.push({ level : 'info', path: filePath });
    }
};

var logger = new bunyan.createLogger({
        name: config.logging.logName,
        streams: createStreams()
    });

module.exports = logger;