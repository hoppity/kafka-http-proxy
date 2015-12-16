var express         = require('express'),
    app             = express(),
    bodyParser      = require('body-parser'),
    config          = require('./config'),
    log             = require('./logger.js'),
    logger          = log.logger,

    fs              = require('fs'),
    morgan          = require('morgan'),
    accessLogStream = fs.createWriteStream(__dirname + '/' + config.accessLogPath, {flags: 'a'});

logger.info('Starting application...');

app.use(morgan('combined', { stream: accessLogStream }));

app.use(require('express-domain-middleware'));

app.use(function(req, res, next) {
    var request = {
        url: req.url,
        method: req.method
    };

    var response = res.err || {};

    logger.trace({req: request, res: response}, 'Received request.');
    next();
});

app.use(bodyParser.json({ type: 'application/*+json' }));

require('./controllers/topics')(app);
require('./controllers/consumers')(app);

// handle any errors that manage to slip through the cracks
process.on('uncaughtException', function(err) {
    logger.error({err : err}, 'unprocessed and unhandled exception!');
});

app.use(function errorHandler(err, req, res, next) {
    logger.error({method: req.method, url: req.url}, 'error on request ');
    if (!res.headersSent) {
        return next(err);
    }
    res.status(500).json({ 'error_code': 500, 'message': err });
});

// setup the sockets
var server = require('http').createServer(app);
require('./sockets/consumers')(server);

server.listen(config.port, function () {
    logger.info('Application listening on port ' + config.port);
});
