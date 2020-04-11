#!/bin/bash


cd ~
echo "4. List created topics"
echo " "
# We can now see that topic, 'test', if we run the list topic command:
kafka-topics.sh --list --zookeeper localhost:2181


