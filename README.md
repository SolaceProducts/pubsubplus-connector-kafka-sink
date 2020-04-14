[![Build Status](https://travis-ci.org/SolaceLabs/pubsubplus-connector-kafka-sink.svg?branch=development)](https://travis-ci.org/SolaceDev/pubsubplus-connector-kafka-sink)

# PubSub+ Connector Kafka Sink

This project provides a Kafka to Solace PubSub+ Event Broker [Sink Connector](//kafka.apache.org/documentation.html#connect_concepts) (adapter) that makes use of the [Kafka Connect API](//kafka.apache.org/documentation/#connect).

> Note: there is also a PubSub+ Kafka Source Connector available from the [PubSub+ Connector Kafka Source]() GitHub repository.

Contents:

  * [Overview](#overview)
  * [Use Cases](#use-cases)
  * [Downloads](#downloads)
  * [Quick Start](#quick-start)
  * [Parameters](#parameters)
  * [User Guide](#user-guide)
    + [Deployment](#deployment)
    + [Troubleshooting](#troubleshooting)
    + [Event Processing](#event-processing)
    + [Performance and reliability considerations](#performance-and-reliability-considerations)
    + [Security Considerations](#security-considerations)
  * [Developers Guide](#developers-guide)

## Overview

The Solace/Kafka adapter consumes Kafka topic records and streams them to the PubSub+ Event Mesh as topic and/or queue data events. 

The connector was created using PubSub+ high performance Java API to move data to PubSub+.

## Use Cases

#### Protocol and API messaging transformations

Unlike many other message brokers, the Solace PubSub+ Event Broker supports transparent protocol and API messaging transformations. Messages that reach the event broker are not limited to being consumed from the event broker only by Java clients using the same Java API libraries that were used to send the messages to the event broker. Solace PubSub+ supports transparent interoperability with many message transports and languages/APIs. 

As the following diagram shows, any Kafka topic (keyed or non-keyed) sink record is instantly available for consumption by a consumer that uses one of the Solace supported open standards languages or transport protocols.

![Messaging Transformations](/doc/images/KSink.png)

#### Tying Kafka into the PubSub+ Event Mesh

The [PubSub+ Event Mesh](//docs.solace.com/Solace-PubSub-Platform.htm#PubSub-mesh) is a clustered group of PubSub+ Event Brokers, which appears to individual services (consumers or producers of data events) to be a single transparent event broker and it routes data events in real-time to any service that is part of the Event Mesh. The Solace PubSub+ brokers can be any of the three categories: dedicated extreme performance hardware appliances, high performance software brokers that are deployed as software images (deployable under most Hypervisors, Cloud IaaS and PaaS layers and in Docker) or provided as a fully managed Cloud MaaS (Messaging as a Service). 

By applications registering interest in receiving events, the entire Event Mesh becomes aware of the registration request and will know how to securely route the appropriate events generated by the Solace Sink Connector.

![Messaging Transformations](/doc/images/EventMesh.png)

The PubSub+ Sink Connector will be able to move a new Kafka record to any downstream service via a single connector.

#### Distributing messages to IoT devices

PubSub+ supports bi-directional messaging and the unique addressing of millions of devices through fine-grained filtering. Using the Sink Connector, messages created from Kafka records can be efficiently distributed to a controlled set of destinations.

![Messaging Transformations](/doc/images/IoT-Command-Control.png)


## Downloads

ZIP or TAR packaged PubSub+ Kafka Sink Connector is available from the [downloads](//solacedev.github.io/pubsubplus-connector-kafka-sink/downloads/) page.

The package includes jar libraries, documentation with license information and sample property files. Download and expand it into a directory that is on the `plugin.path` of your connect-standalone or connect-distributed properties file.

## Quick Start

This example demonstrates an end-to-end scenario similar to the [Protocol and API messaging transformations](#protocol-and-api-messaging-transformations) use case, using the WebSocket API to receive an exported Kafka record as a message at the PubSub+ event broker.

It builds on the open source [Apache Kafka Quickstart tutorial](//kafka.apache.org/quickstart) and will walk through how to get started in a standalone environment for development purposes. For setting up a distributed environment for production purposes refer to the User Guide section.

> Note: the steps are similar if using [Confluent Kafka](//www.confluent.io/download/); there may be difference in the root directory where the Kafka binaries (`bin`) and properties (`etc/kafka`) are located.

**Step 1**: Install Kafka. Follow the [Apache tutorial](//kafka.apache.org/quickstart#quickstart_download) to download the Kafka release code, start the Zookeeper and Kafka servers in separate command line sessions, then create a topic named `test` and verify it exists.

**Step 2**: Install PubSub+ Sink Connector. Designate and create a directory for the PubSub+ Sink Connector - assuming it is named `connectors`. Edit `config/connect-standalone.properties` and ensure the `plugin.path` parameter value includes the absolute path of the `connectors` directory.
[Download]( https://solacedev.github.io/pubsubplus-connector-kafka-sink/downloads ) and expand the PubSub+ Sink Connector into the `connectors` directory.

**Step 3**: Acquire access to a PubSub+ message broker. If you don't already have one available, the easiest option is to get a free-tier service in a few minutes in [PubSub+ Cloud](//solace.com/try-it-now/) , following the [Creating Your First Messaging Service] (https://docs.solace.com/Solace-Cloud/ggs_signup.htm) guide. 

**Step 4**: Configure the PubSub+ Sink Connector:

a) Locate the following connection information of your messaging service for the "Solace Java API" (this is what the connector is using inside):
* Username, Password, Message VPN, one of the Host URIs;

b) edit the PubSub+ Sink Connector properties file located at `connectors/pubsubplus-connector-kafka-sink-<version>/etc/solace_sink.properties`  updating following respective parameters so the connector can access the PubSub+ event broker:
* `sol.username`, `sol.password`, `sol.vpn_name`, `sol.host`;

c) Note the configured source and destination information: `topics` is the Kafka source topic (`test`), created in Step 1 and the `sol.topics` parameter specifies the destination topic on PubSub+ (`sinktest`).

**Step 5**: Start the connector in standalone mode. In a command line session run:
```sh
bin/connect-standalone.sh \
config/connect-standalone.properties \
connectors/pubsubplus-connector-kafka-sink-<version>/etc/solace_sink.properties
```
After startup, logs shall eventually contain following line:
```
================Session is Connected
```

**Step 6**: To watch messages arriving into PubSub+, we will use the "Try Me!" test service of the browser-based administration console to subscribe to messages to the `sinktest` topic. Behind the scenes, "Try Me!" is using the WebSocket API from JavaScript code.

* If you are using PubSub+ Cloud for your messaging service follow the [Trying Out Your Messaging Service guide](//docs.solace.com/Solace-Cloud/ggs_tryme.htm).

* If using an existing event broker, log in to its [PubSub+ Manager admin console](//docs.solace.com/Solace-PubSub-Manager/PubSub-Manager-Overview.htm#mc-main-content) and follow the [How to Send and Receive Test Messages guide](//docs.solace.com/Solace-PubSub-Manager/PubSub-Manager-Overview.htm#Test-Messages).

In both cases ensure to set the topic to `sinktest`, which the connector is publishing to.

**Step 7**: Demo time! Start to write messages to the Kafka "test" topic. Get back to the Kafka [tutorial](//kafka.apache.org/quickstart#quickstart_send), type and send `Hello world!`.

The "Try Me!" consumer from Step 6 should now display the new message arriving to PubSub+ through the PubSub+ Kafka Sink Connector:
```
Hello world!
```

## Parameters

Connector parameters consist of [Kafka-defined parameters](https://kafka.apache.org/documentation/#connect_configuring) and PubSub+ connector-specific parameters.

Refer to the in-line documentation of the [sample PubSub+ Kafka Sink Connector properties file](/etc/solace_sink.properties) and additional information in the [Configuration](#Configuration) section.

## User Guide

### Deployment

The PubSub+ Sink Connector deployment has been tested on Apache Kafka 2.4 and Confluent Kafka 5.4 platforms. The Kafka software is typically placed under the root directory: `/opt/<provider>/<kafka or confluent-version>`.

Kafka distributions may be available as install bundles, Docker images, Kubernetes deployments, etc. They all support Kafka Connect which includes the scripts, tools and sample properties for Kafka connectors.

Kafka provides two options for connector deployment: [standalone mode and distributed mode](//kafka.apache.org/documentation/#connect_running).

* In standalone mode, recommended for development or testing only, configuration is provided together in the Kafka `connect-standalone.properties` and in the PubSub+ Sink Connector `solace_sink.properties` files and passed to the `connect-standalone` Kafka shell script running on a single worker node (machine), as seen in the [Quick Start](#quick-start).

* In distributed mode, Kafka configuration is provided in `connect-distributed.properties` and passed to the `connect-distributed` Kafka shell script, which is started on each worker node. The `group.id` parameter identifies worker nodes belonging the same group. The script starts a REST server on each worker node and PubSub+ Sink Connector configuration is passed to any one of the worker nodes in the group through REST requests in JSON format.

To deploy the Connector, for each target machine [download]( //solacedev.github.io/pubsubplus-connector-kafka-sink/downloads), and expand the PubSub+ Sink Connector into a directory and ensure the `plugin.path` parameter value in the `connect-*.properties` includes the absolute path to that directory. Note that Kafka Connect, i.e. the `connect-standalone` or `connect-distributed` Kafka shell scripts must be restarted or equivalent action from a Kafka console is required if the PubSub+ Sink Connector deployment is updated.

Some PubSub+ Sink Connector configurations may require the deployment of additional specific files like keystores, truststores, Kerberos config files, etc. It does not matter where these additional files are located, but they must be available on all Kafka Connect Cluster nodes and placed in the same location on all the nodes because they are referenced by absolute location and configured only once through one REST request for all.

#### REST JSON Configuration

First test to confirm the PubSub+ Sink Connector is available for use in distributed mode with the command:
```ini
curl http://18.218.82.209:8083/connector-plugins | jq
```

In this case the IP address is one of the nodes running the distributed mode worker process, the port is default 8083 or as specified in the `rest.port` property in `connect-distributed.properties`. If the connector is loaded correctly, you should see a response similar to:

```
  {
    "class": "com.solace.sink.connector.SolaceSinkConnector",
    "type": "sink",
    "version": "2.0.0"
  },
```

At this point, it is now possible to start the connector in distributed mode with a command similar to:

```ini
curl -X POST -H "Content-Type: application/json" \
             -d @solace_sink_properties.json \
             http://18.218.82.209:8083/connectors
``` 

The connector's JSON configuration file, in this case, is called "solace_sink_properties.json". A sample is available [here](/etc/solace_sink_properties.json), which can be extended with the same properties as described in the [Parameters section](#parameters).

Determine if the Sink Connector is running with the following command:
```ini
curl 18.218.82.209:8083/connectors/solaceSourceConnector/status | jq
```
If there was an error in starting, the details will be returned with this command. 

### Troubleshooting

In standalone mode, connect logs are written to the console. If you do not want the output to console, simply add the "-daemon" option and all output will be directed to the logs directory.

In distributed mode, the logs location is determined by the `connect-log4j.properties` located at the `config` directory in the Apache Kafka distribution or under `etc/kafka/` in the Confluent distribution.

If logs are redirected to the standard output, here is a sample log4j.properties snippet to direct them to a file:
```
log4j.rootLogger=INFO, file
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=/var/log/kafka/connect.log
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=[%d] %p %m (%c:%L)%n
log4j.appender.file.MaxFileSize=10MB
log4j.appender.file.MaxBackupIndex=5
log4j.appender.file.append=true
```

To troubleshoot PubSub+ connection issues, increase logging level to DEBUG by adding following line:
```
log4j.logger.com.solacesystems.jcsmp=DEBUG
```
Ensure to set it back to INFO or WARN for production.

### Event Processing

#### Record processors

There are many ways to map topic, partition, key and values of Kafka records to PubSub+ messages, depending on the application.

The PubSub+ Sink Connector comes with three sample record processors that can be used as is, or as a starting point to develop a customized record processor.

* **SolSimpleRecordProcessor** - takes the Kafka sink record as a binary payload with a binary schema for the value, which becomes the PubSub+ message payload. The key and value schema can be changed via the configuration file.

* **SolSimpleKeyedRecordProcessor** - a more complex sample that allows the flexibility of mapping the sink record key to PubSub+ message contents. In this sample, the kafka key is set as a Correlation ID in the Solace messages. The option of no key in the record is also possible.

* **SolDynamicDestinationRecordProcessor** - by default, the Sink Connector will send messages to destinations (Topic or Queues) defined in the configuration file. This example shows how to route each Kafka record to a potentially different PubSub+ topic based on the record binary payload. In this imaginary transportation example the records are distributed to buses listening to topics like `ctrl/bus/<busId>/<command>`, where the <busId> is encoded in the first 4 bytes in the record value and <command> in the rest. Note that `sol.dynamic_destination=true` must be specified in the configuration file to enable this mode (otherwise destinations are taken from sol.topics or sol.queue).

In all processors the original Kafka topic, partition and offset are included for reference in the PubSub+ Message as UserData in the Solace message header, sent as a "User Property Map". The message dump is similar to:
```
Destination:                            Topic 'sinktest'
AppMessageType:                         ResendOfKafkaTopic: test
Priority:                               4
Class Of Service:                       USER_COS_1
DeliveryMode:                           DIRECT
Message Id:                             4
User Property Map:                      3 entries
  Key 'k_offset' (Long): 0
  Key 'k_topic' (String): test
  Key 'k_partition' (Integer): 0

Binary Attachment:                      len=11
  48 65 6c 6c 6f 20 57 6f    72 6c 64                   Hello.World
```

The desired record processor is loaded at runtime based on the configuration of the JSON or properties configuration file, for example:
```
sol.record_processor_class=com.solace.sink.connector.recordprocessor.SolSimpleRecordProcessor
```

It is possible to create more custom record processors based on you Kafka record requirements for keying and/or value serialization and the desired format of the PubSub+ event message. Simply add the new record processor classes to the project. The desired record processor is installed at run time based on the configuration file. 

Refer to the [Developers Guide](#developers-guide) for more information about building the Sink Connector and extending record processors.

#### Message Replay

By default, the Sink Connector will start sending events based on the last Kafka topic offset that was flushed before the connector was stopped. It is possible to use the Sink Connector to replay messages from the Kafka topic.

Adding a configuration entry allows the Sink Connector to start processing from an offset position that is different from the last offset that was stored before the connector was stopped:
```
sol.kafka_replay_offset=<offset>
```

A value of 0 will result in the replay of the entire Kafka Topic. A positive value will result in the replay from that offset value for the Kafka Topic. The same offset value will be used against all active partitions for that Kafka Topic.

### Performance and reliability considerations

#### Sending to PubSub+ Topics

It is recommended to use PubSub+ Topics if high throughput is required and the Kafka Topic is configured for high performance. Message duplication and loss will mimic the underlying reliability and QoS configured for the Kafka topic.

#### Sending to PubSub+ Queue

When Kafka records reliability is critical, it is recommended to mimic this reliability and configure the Sink Connector to send records to the Event Mesh using PubSub+ queues at the cost of reduced throughput.

A PubSub+ queue guarantees order of delivery, provides High Availability and Disaster Recovery (depending on the setup of the PubSub+ brokers) and provides an acknowledgment to the connector when the event is stored in all HA and DR members and flushed to disk. This is a higher guarantee than is provided by Kafka even for Kafka idempotent delivery.

The connector is using local transactions to deliver to the queue by default - the transaction will be committed if messages are flushed by Kafka Connect (see below how to tune flush interval) or the outstanding messages size reaches the `sol.autoflush.size` (default 200) configuration.

Note that generally one connector can only send to one queue.

##### Recovery from Connect or Kafka Broker Fail

The Kafka Connect API automatically keeps track of the offset that the Sink Connector has read and processed. If the connector stops or is restarted, the Connect API will start passing records to the connector based on the last saved offset.

The time interval to save the last offset can be tuned via the `offset.flush.interval.ms` parameter (default 60,000 ms) in the worker's `connect-distributed.properties` configuration file.

Recovery may result in duplicate PubSub+ events published to the Event Mesh. As described [above](#record-processors), the Solace message header "User Property Map" contains all the Kafka unique record information which enables identifying and filtering duplicates.

#### Multiple Workers

The Sink Connector will scale when more performance is required. Throughput is limited by a single instance of the Connect API - the Kafka Broker can produce records and the Solace PubSub+ broker can consume messages at a far greater rate.

Multiple connector tasks are automatically deployed and spread across all available Connect workers simply by indicating the number of desired tasks in the connector configuration file.

There are no special configuration requirements for PubSub+ queue or topics to support scaling.

On the Kafka side the Connect API will automatically use a Kafka consumer group to allow moving of records from multiple topic partitions in parallel.

### Security Considerations

The security setup and operation between the PubSub+ broker and the Sink Connector and Kafka broker and the Sink Connector operate completely independently.
 
The Sink Connector supports both PKI and Kerberos for more secure authentication beyond the simple user name/password, when connecting to the PubSub+ event broker.

The security setup between the Sink Connector and the Kafka brokers is controlled by the Kafka Connect libraries. These are exposed in the configuration file as parameters based on the Kafka-documented parameters and configuration. Please refer to the [Kafka documentation](//docs.confluent.io/current/connect/security.html) for details on securing the Sink Connector to the Kafka brokers for both PKI/TLS and Kerberos. 

#### PKI/TLS

The PKI/TLS support is [well documented in the Solace Documentation](//docs.solace.com/Configuring-and-Managing/TLS-SSL-Service-Connections.htm), and will not be repeated here. All the PKI required configuration parameters are part of the configuration variable for the Solace session and transport as referenced above in the [Parameters section](#parameters). Sample parameters are found in the included [properties file](/etc/solace_sink.properties). 

#### Kerberos authentication

Kerberos authentication support requires a bit more configuration than PKI since it is not defined as part of the Solace session or transport. 

Typical Kerberos client applications require details about the Kerberos configuration and details for the authentication. Since the Sink Connector is a server application (i.e. no direct user interaction) a Kerberos _keytab_ file is required as part of the authentication, on each Kafka Connect Cluster worker node where the connector is deployed.

The enclosed [krb5.conf](/etc/krb5.conf) and [login.conf](/etc/login.conf) configuration files are samples that will allow automatic Kerberos authentication for the Sink Connector when it is deployed to the Connect Cluster. Together with the _keytab_ file, they must be also available on all Kafka Connect cluster nodes and placed in the same (any) location on all the nodes. The files are then referenced in the Sink Connector properties, for example:
```ini
sol.kerberos.login.conf=/opt/kerberos/login.conf
sol.kerberos.krb5.conf=/opt/kerberos/krb5.conf
```

Following property entry is also required to specify Kerberos Authentication:
```ini
sol.authentication_scheme=AUTHENTICATION_SCHEME_GSS_KRB
```

Additional hints: Kerberos has some very specific requirements to operate correctly.
* DNS must be operating correctly both in the Kafka brokers and on the Solace PS+ broker.
* Time services are recommended for use with the Kafka Cluster nodes and the Solace PS+ broker. If there is too much drift in the time between the nodes Kerberos will fail.
* You must use the DNS name in the Solace PS+ host URI in the Connector configuration file and not the IP address
* You must use the full Kerberos user name (including the Realm) in the configuration property, obviously no password is required. 

## Developers Guide

### Build and test the project

JDK 8 or higher is required for this project.

First, clone this GitHub repo:
```
git clone https://github.com/SolaceProducts/pubsubplus-connector-kafka-sink.git
cd pubsubplus-connector-kafka-sink
```

Then run the build script:
```
gradlew clean build
```

This will create artifacts in the `build` directory, including the deployable packaged PubSub+ Sink Connector archives under `build\distributions`.

An integration test suite is also included, which spins up a Docker-based deployment environment that includes a PubSub+ event broker, Zookeeper, Kafka broker, Kafka Connect. It deploys the connector to Kafka Connect and runs end-to-end tests.
```
gradlew clean integrationTest --tests com.solace.messaging.kafka.it.SinkConnectorIT
```

### Build a new record processor

The processing of a Kafka record to create a PubSub+ message is handled by an interface definition defined in [`SolRecordProcessorIF.java`](/src/main/java/com/solace/sink/connector/SolRecordProcessorIF.java). This is a simple interface that is used to create the Kafka source records from the PubSub+ messages. There are three examples included of classes that implement this interface:

* [SolSimpleRecordProcessor](/src/main/java/com/solace/sink/connector/msgprocessors/SolSimpleRecordProcessor.java)
* [SolSimpleKeyedRecordProcessor](/src/main/java/com/solace/sink/connector/msgprocessors/SolSimpleKeyedRecordProcessor.java)
* [SolDynamicDestinationRecordProcessor](/src/main/java/com/solace/sink/connector/msgprocessors/SolDynamicDestinationRecordProcessor.java)

These can be used as starting points for custom record processor implementations.

More information on Kafka sink connector development can be found here:
- [Apache Kafka Connect](https://kafka.apache.org/documentation/)
- [Confluent Kafka Connect](https://docs.confluent.io/current/connect/index.html)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

See the list of [contributors](../../graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.

## Resources

For more information about Solace technology in general please visit these resources:

- The [Solace Developers website](https://www.solace.dev/)
- Understanding [Solace technology]( https://solace.com/products/tech/)
- Ask the [Solace Community]( https://solace.community/)
