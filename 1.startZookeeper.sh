#!/bin/bash

# Step 0
# As there are two kafka hosted
#  we need to delete some files in /tmp 
#    LOGIN AS ROOT USER TO ISSUE BELOW TWO COMMANDS


echo "Before you begin, open another terminal & login as root"
echo "And run the following two commands"
echo " rm -rf /tmp/zookeeper "
echo " rm -rf /tmp/kafka-logs* "
echo " "
echo -n "Press [ENTER] to continue "
read name

cd ~
# Step 1. Login as user kafka 
#         Delete kafka logs

echo "Deleting log files"
rm -rf /tmp/kafka-logs*
rm -rf /tmp/zookeeper
rm -f  /opt/kafka_2.11-2.0.0/logs/*

# Step 2: Start zookeeper server

# Kafka uses ZooKeeper so you need to first start a ZooKeeper server
#  You can use the convenience script packaged with kafka to get
#    a quick-and-dirty single-node ZooKeeper instance.
echo "Deleted"
echo "Will now start zookeeper"
echo "This terminal will be fully engaged "
echo " "
zookeeper-server-start.sh  /opt/kafka_2.11-2.0.0/config/zookeeper.properties

