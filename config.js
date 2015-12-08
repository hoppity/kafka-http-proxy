var config = require('config-node');

config({env: 'default'});
config();

if (!!process.env.ZOOKEEPER_CONNECT) {
    config.kafka.zkConnect = process.env.ZOOKEEPER_CONNECT;
}

module.exports = config;