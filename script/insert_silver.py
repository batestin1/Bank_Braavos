#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Silver
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports

from pyspark.sql import SparkSession
secret = open("../secrets/secrets.txt", 'r')
secret = list(secret)

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting transaction for silver file...")


#read
print("#"*100)
print("read table address from datalake bronze")
print("#"*100)
address = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/address/").createOrReplaceTempView("address")
print("#"*100)
print("read table account from datalake bronze")
print("#"*100)
account = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/account/").createOrReplaceTempView("account")
print("#"*100)
print("read table account card from datalake bronze")
print("#"*100)
accountcard = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/accountcard/").createOrReplaceTempView("accountcard")
print("#"*100)
print("read table card from datalake bronze")
print("#"*100)
card = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/card/").createOrReplaceTempView("card")
print("#"*100)
print("read table account client from datalake bronze")
print("#"*100)
client = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/client/").createOrReplaceTempView("client")
print("#"*100)
print("read table client from datalake bronze")
print("#"*100)
rent = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/bronze/parquet/rent/").createOrReplaceTempView("rent")
print("#"*100)
print("read table rent from datalake bronze")
print("#"*100)



#transform
print("#"*100)
print("Performing data deduplication")
print("#"*100)
address = spark.sql("""SELECT * FROM 
(SELECT id, data_original as original_date, date_processing, currency_city, city_of_birth, country_of_birth, current_culture, birth_culture, number,  street, postalcode,
 row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM address WHERE TRIM(id) <> '')
WHERE row_id = 1 """)


account = spark.sql("""
SELECT * FROM
(SELECT id, data_original as original_date, date_processing, bank, type, type_profile, bban, aba,
row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM account WHERE TRIM(id) <> '')
WHERE row_id = 1""")

accountcard = spark.sql("""
SELECT * FROM
(SELECT id, data_original as original_date, date_processing, status_account_blocked, card_number, transaction_code, date_transaction, original_transaction_amount,
number_installments_assign, current_installment, currency.time_transaction as time_transaction, currency.describle_transaction as describle_transaction,
currency.currency as currency,
row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM accountcard  WHERE TRIM(id) <> '')
WHERE row_id = 1""")

card = spark.sql("""SELECT * FROM
(SELECT id, data_original as original_date, card_number, total_limit_value, total_limit_used, description_card, security_card, 
REPLACE(expire_card_number, '/','') as expire_card_number, date_processing,
row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM card  WHERE TRIM(id) <> '')
WHERE row_id = 1

""")

client = spark.sql("""SELECT * FROM
(SELECT id, data_original as original_date, itin, name, family, house, words, title, gender, faith, god, date_processing,
row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM client  WHERE TRIM(id) <> '')
WHERE row_id = 1
""")

rent = spark.sql("""SELECT * FROM
(SELECT id, data_original as original_date, date_processing, 
rent_income_value, description_chosen_income_method, total_account_value, current_account_total_value, total_amount_carried_over,
row_number() OVER (PARTITION BY id ORDER BY date_processing) 
as row_id FROM rent  WHERE TRIM(id) <> '')
WHERE row_id = 1
""")
#LOAD#########################################################################################################
print("#"*100)
print("loading account in silver datalake")
print("#"*100)
account.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/account/")
account.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/account/")

print("#"*100)
print("loading account card in silver datalake")
print("#"*100)
accountcard.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/accountcard/")
accountcard.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/accountcard/")

print("#"*100)
print("loading card in silver datalake")
print("#"*100)
card.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/card/")
card.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/card/")

print("#"*100)
print("loading rent in silver datalake")
print("#"*100)
rent.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/rent/")
rent.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/rent/")

print("#"*100)
print("loading client in silver datalake")
print("#"*100)
client.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/client/")
client.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/client/")

print("#"*100)
print("loading address in silver datalake")
print("#"*100)
address.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/orc/address/")
address.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}IronBankBraavos/datalake/silver/parquet/address/")
