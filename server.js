var express = require('express'),
    app = express(),
    bodyParser = require('body-parser'),
    config = require('./config'),
    log = require('./logger.js'),
    logger = log.logger,

    fs = require('fs'),
    morgan = require('morgan'),
    accessLogStream = fs.createWriteStream(__dirname + '/' + config.accessLogPath, {flags: 'a'});

app.use(morgan('combined', { stream: accessLogStream }));

app.use(function(req, res, next) {
    var request = {
        url: req.url,
        method: req.method
    };

    var response = res.err || {};

    logger.debug({req: request, res: response}, config.logging.logName + ' Express Server Info Messages');
    next();
});

app.use(bodyParser.json({ type: 'application/*+json' }));

require('./controllers/topics')(app);
require('./controllers/consumers')(app);


app.use(function errorHandler(err, req, res, next) {
    logger.error(err);
    if (res.headersSent) {
        return next(err);
    }
    res.status(500).json({ 'error_code': 500, 'message': err });
});

app.listen(config.port);
