import dash
from dash import dcc, html
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sqlite3
import warnings
from plotly.subplots import make_subplots

warnings.simplefilter(action='ignore', category=UserWarning)

conn = sqlite3.connect("ALL_FAULTS_INCLUDED.sqlite3")
query_card = 'SELECT * FROM CARD;'

def month_data(df_card_data):
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    df_month = df_card_data['MONTH'].unique()
    # print(df_month)
    # sorted(df_month, key=lambda x: months.index(x.split(",")[0]))
    month_order = sorted(df_month, key=lambda x: (int(x.split(",")[1]), months.index(x.split(",")[0])))

    return month_order
df_card = pd.read_sql_query(query_card, conn)


query_raw='SELECT * FROM RAW'
df_raw=pd.read_sql_query(query_raw,conn)
final_month_list = month_data(df_card)

d = df_card
# d['MONTH'] = pd.Categorical(d['MONTH'])
# d=d[d['MONTH'] == month_data(d)]
print(final_month_list[-13:])
a=d[d["MONTH"].isin(final_month_list[-13:])]

# print(a)
# print(a["MONTH"].unique())
a['MONTH'] = pd.Categorical(a['MONTH'], categories=final_month_list[-13:], ordered=True)
d = a.groupby('MONTH')[['TEST_QUANTITY', 'REJECT_QUANTITY']].sum().reset_index()
# print("qqqq")
print(d["MONTH"].unique())
#
# b['DPT'] = round((b['REJECT_QUANTITY'] / b['TEST_QUANTITY']) * 1000, 0)
#
#
                    # months=final_month_list[0:month+1]
                    # index=final_month_list.index(month)
#
# del b['TEST_QUANTITY']
# del b['REJECT_QUANTITY']
# print(b)

