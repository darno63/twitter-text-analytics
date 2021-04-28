#!/bin/bash
kafka_2.13-2.8.0/bin/zookeeper-server-start.sh -daemon kafka_2.13-2.8.0/config/zookeeper.properties
kafka_2.13-2.8.0/bin/kafka-server-start.sh -daemon kafka_2.13-2.8.0/config/server.properties

