var express = require('express'),
    app = express(),
    bodyParser = require('body-parser');

app.use(bodyParser.json());

require('./controllers/topics')(app);

app.listen(8000);