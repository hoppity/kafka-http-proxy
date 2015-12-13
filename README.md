`# kafka-http-proxy

A proxy that allows applications to use Kafka via HTTP... written in NodeJS.

## But why...?

Native clients for Kafka are tricky. The protocol is moving fast and there is a lot to manage. This project gives applications a bridge so that languages that aren't "supported" can still use Kafka.

The inspiration for this project came from the [Confluent REST Proxy](docs.confluent.io/1.0/kafka-rest/intro.html). We found that project to be too heavy for what we wanted and introduce a lot of lag between messages going from producer to consumer. We have tried to keep the API consistent with that so clients already using it can easily switch over, but wanted to simplify as much as possible.

## Getting started

Get the source...

    git clone https://github.com/hoppity/kafka-http-proxy
    npm install
    export ZOOKEEPER_CONNECT=127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181
    node server.js

Or using docker...

    docker run
        -p 8085:8085 `
        -e "ZOOKEEPER_CONNECT=127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181" `
        --name kafka-http-proxy
        hoppity/kafka-http-proxy

## Configuration

Most configuration is contained in `config/default.json`.

* `kafka`
 * `zkConnect` - the Zookeeper connection string
 * `clientId` - the client identifier for the application
* `logging`
 * `logName` - ?
 * `level` - the level of logging to output
* `accessLogPath` - the path to an Apache standard compliant access log
* `port` - the port to expose the application on
* `consumer`
 * `timeoutMs` - the number of milliseconds to wait between polls before a consumer is deemed inactive and timed-out.

This can be changed or overriden by placing values in another file in the `config` directory and setting the `NODE_ENV` variable (e.g. `production.json` and `export NODE_ENV=production`).

## API

`TODO:` Write about standard errors.

### /topics

#### PUT /topics/{topic}

Creates a topic with the name specified using the default topic creation settings. This will fail if automatic topic creation is disabled in Kafka.

`Input`

* params
 * `topic` - the name of the topic to create

`Output`

* JSON Response
 * `message` - "All created"

E.g.

    $ curl -X PUT http://localhost:8085/topics/test-topic

    {"message":"All created"}

#### POST /topics/{topic}

Publishes a message to the specified topic.

`Input`

* params
 * `topic` - the name of the topic to publish to
* JSON body
 * `records` - array of messages to publish
  * `key`
  * `value`
  * `partition`

E.g.

    $ curl -X POST \
        -H 'Content-Type: application/kafka.binary.v1+json' \
        -d '{"records":[{"value":"1234"}]}' \
        http://localhost:8085/topics/test-topic

    {"offsets":[{"partition":"0","offset":0}]}

### /consumers

#### POST /consumers/{group}

Create a consumer with the specified group name.

`Input`

* params
 * `group` - the name of the consumer group

`Output`

* JSON response
 * `instance_id` - the identifier of the consumer instance
 * `base_uri` - the base URI for consumers to use when polling for messages

E.g.

    $ curl -X POST http://localhost:8085/consumers/test-group

    {
        "instance_id":"3bae5d25-b0bc-4e4a-b937-bdc75ceece39",
        "base_uri":"http://localhost:8085/consumers/test-group/instances/3bae5d25-b0bc-4e4a-b937-bdc75ceece39"
    }

#### GET /consumers/{group}/instances/{instance_id}/topics/{topic}

Consumes messages from the topic on the specified consumer. The URI should be formed by concatenating the base_uri returned when creating the consumer and `/topics/{topic}`.

`Input`

* params
 * `group` - the name of the consumer group
 * `instance_id` - the consumer instance id
 * `topic` - the name of the topic to consume

`Output`

* JSON response
 * Array of message
  * `topic`
  * `partition`
  * `offset`
  * `key`
  * `value`

E.g.

    $ curl http://localhost:8085/consume/test-group/instances/3bae5d25-b0bc-4e4a-b937-bdc75ceece39

    [
        {
            "topic":"test-topic",
            "partition":0,
            "offset":0,
            "key":"",
            "value":"1234"
        }
    ]

#### POST /consumers/{group}/instances/{instance_id}/offsets

Commit the current offset of the consumer instance. The URI should be formed by concatenating the base_uri returned when creating the consumer and `/offsets`.

`Input`

* params
 * `group` - the name of the consumer group
 * `instance_id` - the consumer instance id

`Output`

* JSON Response
 * Empty array

E.g.

    $ curl -X POST http://localhost:8085/consume/test-group/instances/3bae5d25-b0bc-4e4a-b937-bdc75ceece39/offsets

    []


#### DELETE /consumers/{group}/instances/{instance_id}

Shut down a consumer instance. The URI should the base_uri returned when creating the consumer.

`Input`

* params
 * `group` - the name of the consumer group
 * `instance_id` - the consumer instance id

`Output`

* JSON Response
 * Empty object

E.g.

    $ curl -X DELETE http://localhost:8085/consume/test-group/instances/3bae5d25-b0bc-4e4a-b937-bdc75ceece39

    {}
`
