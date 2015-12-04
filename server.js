var express = require('express'),
    app = express(),
    bodyParser = require('body-parser'),
    config = require('config-node')();

app.use(bodyParser.json());

require('./controllers/topics')(app);
require('./controllers/consumers')(app);

app.use(function errorHandler(err, req, res, next) {
    console.error(err);
    if (res.headersSent) {
        return next(err);
    }
    res.status(500).json({ 'error_code': 500, 'message': err });
});

app.listen(config.port);