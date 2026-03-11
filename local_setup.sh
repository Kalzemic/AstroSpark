#/bin/usr/env bash

cd /usr/local/kafka/kafka_2.13-3.2.1

bin/zookeeper-server-start.sh    config/zookeeper.properties &

bin/kafka-server-start.sh        config/server.properties &

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic adql-queries

bin/kafka-console-producer.sh   --broker-list   localhost:9092   --topic adql-queries