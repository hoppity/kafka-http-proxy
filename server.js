var express = require('express'),
    app = express(),
    bodyParser = require('body-parser'),
    config = require('config-node')();

app.use(bodyParser.json());

require('./controllers/topics')(app);
require('./controllers/consumers')(app);

app.listen(config.port);