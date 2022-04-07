#! /usr/bin/env bash

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: run.sh                                                             #
                        #   created: 2022-03-08                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#
echo "START PROJECT"

echo "inform the number of data you want to insert: "
read data

python ../dataset/script/create_json.py $data
python ../script/insert_bronze.py
python ../script/insert_silver.py
python ../script/insert_gold.py
python ../script/metrics.py

