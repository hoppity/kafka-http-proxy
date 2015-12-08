var bunyan = require('bunyan');
var config = require('./config');

var logger = new bunyan.createLogger({
        name: config.logging.logName,
        streams: [
            {
                level: config.logging.level,
                stream : process.stdout
            }]
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
