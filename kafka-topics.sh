#!/usr/bin/env zsh
kafka-topics --bootstrap-server kafka1:29097 --list
echo -e 'Creating kafka topics'
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task1-1 --replication-factor 1 --partitions 1
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task1-2 --replication-factor 1 --partitions 1
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task2 --replication-factor 1 --partitions 1
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task3-1 --replication-factor 1 --partitions 1
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task3-2 --replication-factor 1 --partitions 1
kafka-topics --bootstrap-server kafka1:29097 --create --if-not-exists --topic task4 --replication-factor 1 --partitions 1

echo -e 'Successfully created the following topics:'
kafka-topics --bootstrap-server kafka1:29097 --list
