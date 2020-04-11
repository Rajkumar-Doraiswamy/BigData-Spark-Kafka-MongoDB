#!/bin/bash


cd ~
# Step 3: Create a topic
# Let's create a topic named "test" with a single partition and only one replica:
echo "3. Will create a new topic test"
echo " "
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test


