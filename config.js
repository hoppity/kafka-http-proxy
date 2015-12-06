var config = require('config-node');

config({env: 'default'});
config();

module.exports = config;