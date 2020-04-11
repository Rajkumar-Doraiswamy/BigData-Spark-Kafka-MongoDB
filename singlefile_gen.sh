#!/bin/bash


# Last amended: 24/10/2018
# Myfolder: /home/ashok
# VM:  lubuntu_spark

## Objective:
# 		Generates only one file
# 		Used to generate fake data
# 		for feeding to kafka
#             for use in spark streaming
# 		Press ctrl+c to terminate for-loop

## Usage:
#   Open one terminal and run this file as:
#     	cd /home/ashok/Documents/fake
#    		./file_gen.sh


# 1.0 First delete any existing files in the folder
echo "Deleting existing files from folder: spark/data "
echo "   "
cd /home/ashok/Documents/spark/data
rm -f *.csv
cd ~

touch /home/ashok/Documents/spark/data/1.csv
# 2.0 Add data to file at 10 seconds interval
for i in {1..100}
do
   echo "Creating/appending data to  file: 1.csv . "
   echo " Number of lines in 1.csv: "  "$(wc -l /home/ashok/Documents/spark/data/1.csv)"
   python  /home/ashok/Documents/fake/simpleDataGen.py  >>  /home/ashok/Documents/spark/data/1.csv
  # Next, react after 10 seconds
   sleep 10
done

################# END ###########################