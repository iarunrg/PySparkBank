#Import Cells
# from email import header
# from importlib.resources import path
# from statistics import mode
import findspark
findspark.init()
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, dense_rank, column, date_format, to_date
from pyspark.sql.types import *
# import pandas as pd
# import numpy as np


spark = SparkSession.builder.master("local").appName("SampleBank").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(spark.version)

# bank_df = pd.read_csv("C:\\Arun\\BigData\\Dataset\\bank.csv")
# print(bank_df.head())

bank_df = spark.read.csv(path='C:\\Arun\\BigData\\Dataset\\bank.csv', header=True, inferSchema=True)
bank_df.printSchema()
bank_df.createOrReplaceTempView("Bank_DF")

# Req 1 Fetching Top5 withdrawal
Top5Withdrawal = spark.sql("Select AccountNo, Date, WithdrawalAmt from Bank_DF order by WithdrawalAmt desc limit 5")
Top5Withdrawal.show()
Top5Withdrawal.coalesce(1).write.csv(header=True, mode='overwrite', path="C:\\Arun\\BigData\\Ouptut\\PySpark\\Top5Withdrawal")

# Req 2 Fetching Top 5 Cheque Transactions
Top5Cheque = spark.sql("Select AccountNo, Date, ChqNo, WithdrawalAmt from Bank_DF order by WithdrawalAmt desc limit 5")
Top5Cheque.show()
Top5Cheque.coalesce(1).write.csv(mode='overwrite', header=True ,path='C:\\Arun\\BigData\\Ouptut\\PySpark\\Top5Cheque')

# Req 3 UDF to categorise	

categoryMap = {}
categoryMap.update({"IMPS" : "IMPS"});
categoryMap.update({"RTGS" : "RTGS"});
categoryMap.update({"NEFT" : "NEFT"});
categoryMap.update({"Indiaforensic" :"India Forensic"});
categoryMap.update({"INTERNAL FUND" : "Internal Fund Transfer"});
categoryMap.update({"CASHDEP" : "Cash Deposit"});
categoryMap.update({"VISA" : "Visa"});
categoryMap.update({"RUPAY" : "Rupay"});
categoryMap.update({"MASTERCARD" : "Master Card"});
categoryMap.update({"IRCTC" : "IRCTC"});
categoryMap.update({"Processing Fee" : "Processing Fee"});

def catFinder(category):
    if category is not None:
        for cat in categoryMap.keys():
            if category.__contains__(cat):
                return str(categoryMap.get(cat))
    return "Others"

print(catFinder("TRF FROM  Indiaforensic SERVICES"))

Categorizer = udf(catFinder, StringType())
spark.udf.register(name='Categorizer', f=catFinder)
TransactionDetail = bank_df.withColumn("Category", Categorizer(bank_df.TransactionDetails))
TransactionDetail.show()
TransactionDetail.createOrReplaceTempView("BankData")


TransactionSummary = spark.sql("Select Category, max(WithdrawalAmt), max(DepositAmt), min(WithdrawalAmt), min(DepositAmt), "
				+ "avg(WithdrawalAmt), avg(DepositAmt), count(AccountNo) from BankData group by Category")
TransactionSummary.show()
TransactionSummary.coalesce(1).write.csv(header=True, mode='overwrite', path='C:\\Arun\\BigData\\Ouptut\\PySpark\\TransactionSummary')

# Req 4 Rank Top 5 Deposit for each Category
TransactionDetail.printSchema()
Top5CatTrans = TransactionDetail.withColumn("DenseRank", dense_rank().over(Window.partitionBy("Category").
                    orderBy(TransactionDetail.DepositAmt.desc_nulls_last())))
                    # orderBy(column("DepositAmt").desc_nulls_last())))
Top5CatTrans.createOrReplaceTempView("Top5Summary")
# Top5CatTrans.show()

Top5CatDepositAmt = spark.sql("Select Category, DepositAmt, DenseRank from Top5Summary where DenseRank<=5")
Top5CatDepositAmt.show()
Top5CatDepositAmt.coalesce(1).write.csv(header=True, mode='overwrite', path='C:\\Arun\\BigData\\Ouptut\\PySpark\\Top5CatDepositAmt')

# Req 5 
TransDate = TransactionDetail.withColumn("NewDate", date_format(to_date(
        column("Date"),"dd-MMM-yy"), "MMyy")).withColumn("MonthlyRank", dense_rank().over(Window.partitionBy
                ("NewDate","Category").orderBy(column("DepositAmt").desc_nulls_last())))
TransDate.createOrReplaceTempView("MonthlyTransSummary")
MonthlySummary = spark.sql("Select Category, DepositAmt, MonthlyRank from MonthlyTransSummary where MonthlyRank<=5")
MonthlySummary.show()
MonthlySummary.coalesce(1).write.csv(header=True, mode='overwrite', path="C:\\Arun\\BigData\\Ouptut\\PySpark\\MonthlySummary")