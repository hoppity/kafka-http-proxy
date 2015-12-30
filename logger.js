var bunyan = require('bunyan');
var config = require('./config');
var PrettyStream = require('bunyan-prettystream');
 
var prettyStdOut = new PrettyStream();
prettyStdOut.pipe(process.stdout);

var streams = [];
if (config.logging.stdout && config.logging.stdout.enabled) {
    streams.push({
        level: config.logging.stdout.level,
        type: 'raw',
        stream : prettyStdOut
    });
}
if (config.logging.file && config.logging.file.enabled) {
    streams.push({
        level: config.logging.file.level,
        path: config.logging.file.path,
        type: 'file'
    });
}

var logger = new bunyan.createLogger({
    name: config.logging.logName,
    src: true,
    streams: streams
});

module.exports = {
    logger : logger,

    processError: function(err, msg) {
        logger.error({err: err}, msg);
    },

    processUriError : function(err, msg) {
        logger.error({err:err}, msg);
/*        logger.error({err:{
            message: err.error.message,
            uri: err.request.originalUrl,
            params: err.request.params
        }}, msg); */
    },

    processPublishError : function(err, msg) {
        logger.error({err:{
            message: err.error.message,
            options: err.options
        }}, msg);
    }
};
