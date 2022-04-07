#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Start Bash
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
echo "START PROJECT"

echo "inform the number of data you want to insert: "
read data

python ../dataset/script/create_json.py $data
python ../script/insert_bronze.py
python ../script/insert_silver.py
python ../script/insert_gold.py
python ../script/metrics.py

