#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from pyspark.sql import SparkSession
from tqdm import tqdm
secret = open("../secrets/secrets.txt", 'r')
secret = list(secret)


spark = SparkSession.builder.appName("MyApp").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
print("#"*100)
print("Load from mongo")
df = spark.read.format("mongo").option("uri", "mongodb://localhost/bankBraavos.clients").load()

print("#"*100)

#transform
df.createOrReplaceTempView('df')

print("#"*100)
print("treating account")
print("#"*100)
account = spark.sql(""" SELECT 
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
account_informations.account.bank as bank,
account_informations.account.account_type as type,
account_informations.account.investor_profile as type_profile,
account_informations.account.bban_count as bban,
yearmonthday as data_original,
account_informations.account.aba as aba FROM df
""")

print("#"*100)
print("treating account card")
print("#"*100)
accountcard = spark.sql("""SELECT
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
account_informations.accountcard.status_account_blocked as status_account_blocked,
account_informations.accountcard.card_number as card_number,
account_informations.accountcard.transaction_code as transaction_code,
account_informations.accountcard.date_transaction as date_transaction,
account_informations.accountcard.original_transaction_amount as original_transaction_amount,
account_informations.accountcard.number_installments_assign as number_installments_assign,
account_informations.accountcard.current_installment as current_installment,
yearmonthday as data_original,
account_informations.accountcard as currency FROM df
""")

print("#"*100)
print("treating card")
print("#"*100)
card = spark.sql(""" SELECT
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
yearmonthday as data_original,
account_informations.card.card_number as card_number,
account_informations.card.total_limit_value as total_limit_value,
account_informations.card.total_limit_used as total_limit_used,
account_informations.card.description_card as description_card,
account_informations.card.security_card as security_card,
account_informations.card.expire_card_number as expire_card_number FROM df
""")

print("#"*100)
print("treating rent")
print("#"*100)
rent = spark.sql(""" SELECT
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
yearmonthday as data_original,
account_informations.rent.rent_income_value as rent_income_value,
account_informations.rent.description_chosen_income_method as description_chosen_income_method,
account_informations.rent.total_account_value as total_account_value,
account_informations.rent.current_account_total_value as current_account_total_value,
account_informations.rent.total_amount_carried_over as total_amount_carried_over FROM df
""")

print("#"*100)
print("treating client")
print("#"*100)
people = spark.sql(""" SELECT
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
yearmonthday as data_original,
clients_informations.client.itin as itin,
clients_informations.client.name as name,
clients_informations.client.family as family,
clients_informations.client.house as house,
clients_informations.client.words as words,
clients_informations.client.title as title,
clients_informations.client.gender as gender,
clients_informations.client.faith as faith,
clients_informations.client.god_to_pray as god
FROM df
""")
print("#"*100)
print("treating address")
print("#"*100)
address = spark.sql("""SELECT
monotonically_increasing_id() as id,
date_format(current_date(),'yyyyMMdd') as date_processing,
yearmonthday as data_original,
clients_informations.address.postalcode as postalcode,
clients_informations.address.street as street,
clients_informations.address.number as number,
clients_informations.address.birth_culture as birth_culture,
clients_informations.address.current_culture as current_culture,
clients_informations.address.country_of_birth as country_of_birth,
clients_informations.address.city_of_birth as city_of_birth,
clients_informations.address.currency_city as currency_city
FROM df
""")


######################## load ####################################################################################
print("#"*100)
print("loading account in bronze datalake")
print("#"*100)
account.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/account/")
account.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/account/")
print("#"*100)

print("#"*100)
print("loading account card in bronze datalake")
print("#"*100)
accountcard.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/accountcard/")
accountcard.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/accountcard/")
print("#"*100)


print("#"*100)
print("loading card in bronze datalake")
print("#"*100)
card.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/card/")
card.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/card/")
print("#"*100)

print("#"*100)
print("loading rent in bronze datalake")
print("#"*100)
rent.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/rent/")
rent.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/rent/")
print("#"*100)

print("#"*100)
print("loading client in bronze datalake")
print("#"*100)
people.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/client/")
people.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/client/")
print("#"*100)


print("#"*100)
print("loading address in bronze datalake")
print("#"*100)
address.write.mode("append").format("orc").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/orc/address/")
address.write.mode("append").format("parquet").partitionBy("date_processing").save(f"{secret[0]}/IronBankBraavos/datalake/bronze/parquet/address/")
print("#"*100)