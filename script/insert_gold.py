#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Gold
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from pyspark.sql import SparkSession
secret = open("../secrets/secrets.txt", 'r')
secret = list(secret)

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting transaction for gold file...")

###################client########################################

#read
print("#"*100)
print("read address from silver datalake")
print("#"*100)
address = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/address/").createOrReplaceTempView("address")
print("#"*100)
print("read account from silver datalake")
print("#"*100)
account = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/account/").createOrReplaceTempView("account")
print("#"*100)
print("read account card from silver datalake")
print("#"*100)
accountcard = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/accountcard/").createOrReplaceTempView("accountcard")
print("#"*100)
print("read card from silver datalake")
print("#"*100)
card = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/card/").createOrReplaceTempView("card")
print("#"*100)
print("read client from silver datalake")
print("#"*100)
client = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/client/").createOrReplaceTempView("client")
print("#"*100)
print("read rent from silver datalake")
print("#"*100)
rent = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/rent/").createOrReplaceTempView("rent")

################################### JOIN ##################################################################################


print("#"*100)
print("Creating a unified foundation")
print("#"*100)

full = spark.sql("""SELECT
monotonically_increasing_id() as id, AD.date_processing, AD.original_date,
C.itin, C.name, C.family, C.house, C.words, C.title, C.gender, C.faith, C.god,
CONCAT(AD.currency_city," ", AD.current_culture," ",AD.number," ", AD.street," ",AD.postalcode) as address,
AC.bank, AC.type, AC.type_profile as profile, AC.bban, AC.aba,
ACA.status_account_blocked as status_account, ACA.transaction_code, date_format(ACA.date_transaction, 'yyyyMMdd') as date_transaction, ACA.original_transaction_amount, ACA.number_installments_assign, ACA.current_installment, REPLACE(ACA.time_transaction, ":", "") as time_transaction, ACA.describle_transaction, ACA.currency,
CONCAT(ACA.card_number,"-", CA.total_limit_value,"-", CA.total_limit_used,"-", CA.description_card, "-", CA.security_card, "-", CA.expire_card_number) as info_card,
RE.rent_income_value, RE.description_chosen_income_method, RE.total_account_value, RE.current_account_total_value, RE.total_amount_carried_over
FROM address AD INNER JOIN client C ON AD.id == C.id 
INNER JOIN account AC ON AD.id == AC.id 
INNER JOIN accountcard ACA ON AD.id == ACA.id
INNER JOIN card CA ON AD.id == CA.id
INNER JOIN rent RE ON AD.id == RE.id
ORDER BY id ASC
""")

##########################LOAD################

print("#"*100)
print("loading the full table in gold datalake")
print("#"*100)
full.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/gold/orc/full/")
full.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/gold/parquet/full/")

