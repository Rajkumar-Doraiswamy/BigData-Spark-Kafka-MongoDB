#!/bin/bash


echo "Producing messages to topic test"
echo "================================"
echo " "
echo "Messages are being posted from file"
echo "File: car.data"


kafka-console-producer.sh --broker-list localhost:9092 --topic   test
