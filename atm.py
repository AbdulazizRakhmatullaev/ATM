import pyodbc as db
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

conn = db.connect(
    """DRIVER={ODBC Driver 17 for SQL Server}; 
        SERVER=localhost; 
        DATABASE=practice01; 
        UID=sa; 
        PWD=GetAccessToSql!"""
)
cursor = conn.cursor()


def ingest_to_sqltable():
    url = "https://cbu.uz/ru/"
    html_txt = requests.get(url).text
    soup = BeautifulSoup(html_txt, "html.parser")
    tags = soup.find_all("div", class_="exchange__item_value")
    dt = {}

    for name, rate in tags:
        dt[name.text] = float(rate.text[3:])

    # create table for currency
    table_create = """
                if object_id(N'dbo.currencies', N'U') is null
                create table currencies(
                    currency char(3),
                    rate float,
                    date datetime)
            """
    truncate_qr = "TRUNCATE TABLE currencies"
    exs = [table_create, truncate_qr]

    query = f"INSERT INTO currencies (currency, rate, date) VALUES (?, ?, ?)"
    insert_values = [(currency, rate, datetime.now()) for currency, rate in dt.items()]

    for ex in exs:
        cursor.execute(ex)
    cursor.executemany(query, insert_values)
    conn.commit()
    cursor.close()


ingest_to_sqltable()

# get inserted data from sql
sql_query = pd.read_sql("""select * from currencies""", conn)
df = pd.DataFrame(sql_query, columns=sql_query.columns)


class ATM:
    def __init__(self, balance=0.00):
        self.__balance = balance

    def deposit(self, amount, currency):
        print("\nDeposit Process...")
        print(f"Your balance: {self.__balance}")
        rate = df.loc[df["currency"] == currency, "rate"].values[0]
        amount = amount * rate
        self.__balance += amount
        res = f"Your deposit amount = {amount}\nYour currency = {currency}\nYour Rate = {rate}\nYour balance = {self.__balance}"
        return res

    def withdraw(self, amount):
        print("\nWithdrawing Process...")
        if amount <= self.__balance:
            self.__balance -= amount
            res = f"You withdrew {amount}\nYour balance is = {self.__balance}"
            return res
        else:
            res = "Insufficient funds dummy"
            return res


# check
user1 = ATM()
user1._ATM__balance = 500.001

print(user1.deposit(100, "USD"))
print(user1._ATM__balance)

print(user1.withdraw(1000000))
print(user1._ATM__balance)
