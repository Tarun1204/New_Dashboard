# Import required libraries
import pandas as pd
import numpy as np
import os
import datetime
# import dash_html_components as html
from dash import html
from dash import html, callback, Input, Output, State
import plotly.express as px
from dash import dash_table
import plotly.graph_objs as go

# import plotly.graph_objects as go
import warnings
import re
from Summary_table import table_summary_highlights, table_summary_select_all, table_summary_product

# from sqlalchemy import create_engine
import sqlite3
from functools import lru_cache
# from app import df_raw, df_mcm, df_card, df_smr
warnings.simplefilter(action='ignore', category=UserWarning)
pd.options.mode.chained_assignment = None

basedir = os.path.abspath(os.path.dirname(__file__))

# sql_data.cache_clear()


def alpha_num_order(string):
    return ''.join([format(int(x), '05d') if x.isdigit()
                    else x for x in re.split(r'(\d+)', string)])


def month_data(df_card_data):
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    df_month = df_card_data['MONTH'].unique()
    sorted(df_month, key=lambda x: months.index(x.split(",")[0]))
    final_month_list = sorted(df_month, key=lambda x: (int(x.split(",")[1]), months.index(x.split(",")[0])))

    return final_month_list


@lru_cache(maxsize=128)  # Set the maximum cache size
def sql_data(dataframe):
    modules_f2 = ['2KW SMR', '3KW SMR', '4KW SMR', 'SMR_2KW_SOLAR', 'SMR_3KW_SOLAR', 'CHARGER', 'M1000', 'M2000',
                  'WCBMS', '2KW SUN MOBILITY', 'SMR_1.1KW', 'CHARGER_1.1KW', 'DCIO_F2']
    engine = sqlite3.connect('sql_data/ALL_FAULTS_INCLUDED.sqlite3')
    df_raw12 = pd.read_sql('SELECT * FROM RAW', engine)  # read the entire table
    month_order= month_data(df_raw12)
    months=month_order[-13:]
    # print(months)
    df_raw1 = df_raw12[df_raw12["MONTH"].isin(months)]

    df_card12 = pd.read_sql('SELECT * FROM Card', engine)
    df_card1 = df_card12[df_card12["MONTH"].isin(months)]

    df_smr12 = pd.read_sql('SELECT * FROM SMR', engine)
    # df_smr1['MONTH'] = pd.Categorical(df_smr1["MONTH"], categories=month_order, ordered=True)
    df_smr1 = df_smr12[df_smr12["MONTH"].isin(months)]

    df_mcm1 = pd.read_sql('SELECT * FROM MCM', engine)
    # df_mcm1['MONTH'] = pd.Categorical(df_mcm1["MONTH"], categories=month_order, ordered=True)
    # df_mcm1 = df_mcm1[df_mcm1["MONTH"].isin(months)]

    # df_raw1 = pd.read_sql('SELECT * FROM Faults_data', engine)  # read the entire table
    df_raw1_f1 = df_raw1.loc[~df_raw1['PRODUCT_NAME'].isin(modules_f2)]
    df_raw1_f2 = df_raw1.loc[df_raw1['PRODUCT_NAME'].isin(modules_f2)]

    # df_card1 = pd.read_sql('SELECT * FROM Card_data', engine)
    df_card1_f1 = df_card1.loc[~df_card1['PRODUCT'].isin(modules_f2)]
    df_card1_f2 = df_card1.loc[df_card1['PRODUCT'].isin(modules_f2)]
    # df_smr1 = pd.read_sql('SELECT * FROM SMR_data', engine)
    # df_mcm1 = pd.read_sql('SELECT * FROM MCM_data', engine)
    f1_data = df_card1.loc[~df_card1['PRODUCT'].isin(modules_f2)]
    f2_data = df_card1.loc[df_card1['PRODUCT'].isin(modules_f2)]

    card_product_total = df_card1.groupby(['PRODUCT'])[
        ['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY', 'FTY_PERCENT']].sum()
    card_product_total_f1 = f1_data.groupby(['PRODUCT'])[
        ['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY', 'FTY_PERCENT']].sum()
    card_product_total_f2 = f2_data.groupby(['PRODUCT'])[
        ['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY', 'FTY_PERCENT']].sum()

    card_part_code_month = df_card1.groupby(['PRODUCT', 'PART_CODE', 'DESIGN', 'MONTH'])[['TEST_QUANTITY',
                                                                                          'PASS_QUANTITY',
                                                                                          'REJECT_QUANTITY',
                                                                                          'FTY_PERCENT']].sum()\
        .reset_index()
    card_design_month = df_card1.groupby(['PRODUCT', 'DESIGN', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY',
                                                                          'REJECT_QUANTITY',]].sum() \
        .reset_index()
    card_part_code_month_f1 = f1_data.groupby(['PRODUCT', 'PART_CODE', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY',
                                                                                  'REJECT_QUANTITY', 'FTY_PERCENT']] \
        .sum().reset_index()
    card_part_code_month_f2 = f2_data.groupby(['PRODUCT', 'PART_CODE', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY',
                                                                                 'REJECT_QUANTITY', 'FTY_PERCENT']] \
        .sum().reset_index()

    card_month = df_card1.groupby(['PRODUCT', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY',
                                                         'FTY_PERCENT']].sum().reset_index().set_index('PRODUCT')
    card_design = df_card1.groupby(['PRODUCT', 'MONTH', 'DESIGN'])[['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY',
                                                         'FTY_PERCENT']].sum().reset_index().set_index('PRODUCT')
    card_month_f1 = f1_data.groupby(['PRODUCT', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY',
                                                           'FTY_PERCENT']].sum().reset_index().set_index('PRODUCT')
    card_month_f2 = f2_data.groupby(['PRODUCT', 'MONTH'])[['TEST_QUANTITY', 'PASS_QUANTITY', 'REJECT_QUANTITY',
                                                          'FTY_PERCENT']].sum().reset_index().set_index('PRODUCT')
    df_smr_filter = df_smr1.groupby('PRODUCT').sum()
    df_smr_month = df_smr1.groupby(['PRODUCT', 'MONTH']).sum().reset_index().set_index('PRODUCT')
    df_smr_part_no = df_smr1.groupby(['PRODUCT', 'MONTH', 'PART_CODE']).sum().reset_index().set_index('PRODUCT')
    df_mcm_filter = df_mcm1.groupby('PRODUCT').sum()
    df_mcm_month = df_mcm1.groupby(['PRODUCT', 'MONTH']).sum().reset_index().set_index('PRODUCT')
    df_mcm_part_no = df_mcm1.groupby(['PRODUCT', 'MONTH', 'PART_CODE']).sum().reset_index().set_index('PRODUCT')
    # card_design = df_card1.groupby(['PRODUCT', 'MONTH', 'DESIGN']).sum().reset_index().set_index('DESIGN')

    if dataframe == "raw":
        data = df_raw1
        return data
    if dataframe == "card_design":
        data = card_design_month
        return data
    elif dataframe == "raw_f1":
        data = df_raw1_f1
        return data
    elif dataframe == "raw_f2":
        data = df_raw1_f2
        return data
    elif dataframe == "card":
        data = df_card1
        return data
    elif dataframe == "card_f1":
        data = df_card1_f1
        return data
    elif dataframe == "card_f2":
        data = df_card1_f2
        return data
    elif dataframe == "smr":
        data = df_smr1
        return data
    elif dataframe == "mcm":
        data = df_mcm1
        return data
    elif dataframe == "card_total":
        data = card_product_total
        return data
    elif dataframe == "card_total_f1":
        data = card_product_total_f1
        return data
    elif dataframe == "card_total_f2":
        data = card_product_total_f2
        return data
    elif dataframe == "card_part":
        data = card_part_code_month
        return data
    elif dataframe == "card_part_f1":
        data = card_part_code_month_f1
        return data
    elif dataframe == "card_part_f2":
        data = card_part_code_month_f2
        return data
    elif dataframe == "card_month":
        data = card_month
        return data
    elif dataframe == "card_month_f1":
        data = card_month_f1
        return data
    elif dataframe == "card_month_f2":
        data = card_month_f2
        return data
    elif dataframe == "smr_total":
        data = df_smr_filter
        return data
    elif dataframe == "smr_month":
        data = df_smr_month
        return data
    elif dataframe == "smr_part":
        data = df_smr_part_no
        return data
    elif dataframe == "mcm_total":
        data = df_mcm_filter
        return data
    elif dataframe == "smr_month":
        data = df_mcm_month
        return data
    elif dataframe == "mcm_part":
        data = df_mcm_part_no
        return data


def colour_condition(a):
    df_colour = a
    column_name = df_colour.columns
    size = len(column_name)
    # print(column_name)

    # if 'id' in df_colour:
    #     df_numeric_columns = df_colour.select_dtypes('number').drop(['id'], axis=1)
    # else:
    #     df_numeric_columns = df_colour.select_dtypes('number')
    # styles = []

    if size == 6:
        column_1 = column_name[2]
        column_2 = column_name[4]
        return [
            {
                'if': {
                    'filter_query': '{{{column1}}} > {{{column2}}}'.format(column1=column_1, column2=column_2),
                    'column_id': column_2
                },
                'backgroundColor': '#3D9970',
                'color': 'white'
            }, {
                'if': {
                    'filter_query': '{{{column1}}} < {{{column2}}}'.format(column1=column_1, column2=column_2),
                    'column_id': column_2
                },
                'backgroundColor': 'tomato',
                'color': 'white'
            }
        ]


colors = {
    # For black backgroung
    # 'background': 'rgb(50, 50, 50)',
    # 'text':  'white'      # '#7FDBFF'
    'background': 'white',
    'text':  'black'      # '#7FDBFF'
}


# TASK 2:
# Add a callback function for `Products` as input, `Faults` as output
# Cache clearing function
def clear_cache():
    sql_data.cache_clear()
    print("Cache cleared.")


# Cache clearing logic
def clear_cache_logic(n_clicks, store_data):
    if n_clicks is not None and n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    return store_data


@callback(
    # [Output('navbar-output', 'children'),
    #  Output('clear-cache-store', 'data')],
    Output('clear-cache-store', 'data'),
    [Input('clear-cache-button', 'n_clicks')],
    [State('clear-cache-store', 'data')]
)
def update_cache_clear(n_clicks, store_data):
    store_data = clear_cache_logic(n_clicks, store_data)
    # return "Cache cleared.", store_data
    return store_data


@callback(Output(component_id='fty_all', component_property='style'),
          Input(component_id='fty_all', component_property='children'))
def fty_style(fty_value):
    if fty_value is not None:
        fty_val = fty_value.replace('%', '')
        value = float(fty_val)
        # print(value)
        if value >= 98.5:
            return {
                'textAlign': 'center',
                'color': 'lime', 'margin-top': '-18px',
                'fontSize': 40
            }
        else:
            return {
                'textAlign': 'center',
                'color': 'red', 'margin-top': '-18px',
                'fontSize': 40
            }
    # print(fty_value)
    # print(type(fty_value))
    return {
        'textAlign': 'center',
        'color': 'red', 'margin-top': '-18px',
        'fontSize': 40
    }


@callback(
    [Output(component_id='tested_all', component_property='children'),
     Output(component_id='pass_all', component_property='children'),
     Output(component_id='fail_all', component_property='children'),
     Output(component_id='fty_all', component_property='children'),
     Output(component_id='dpt_all', component_property='children')],
    [Input(component_id='Product_all', component_property='value'),
     Input(component_id='Design', component_property='value'),
     Input(component_id='Month_all', component_property='value'),
     Input(component_id='clear-cache-button', component_property='n_clicks')],
    [State('clear-cache-store', 'data')
     ])
def test_count_4(product,design_no, month, n_clicks, store_data):
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    total = sql_data("card_total")  # for total
    month_wise = sql_data("card_month")  # month vise card
    design_wise = sql_data("card_design")  # for design data
    # print(design_wise)

    if product is None:
        if month is None or len(month) == 0:
            if design_no == 'all_values':
                tested_value = total['TEST_QUANTITY'].sum()
                pass_value = total['PASS_QUANTITY'].sum()
                fail_value = total['REJECT_QUANTITY'].sum()
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
            else:
                tested_value = design_wise.loc[design_wise['DESIGN'] == design_no]['TEST_QUANTITY'].sum()
                pass_value = design_wise.loc[design_wise['DESIGN'] == design_no]['PASS_QUANTITY'].sum()
                fail_value = design_wise.loc[design_wise['DESIGN'] == design_no]['REJECT_QUANTITY'].sum()
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
        else:
            # print("month is not none")
            # print(len(month))
            if design_no == 'all_values':
                tested_value = month_wise.loc[month_wise['MONTH']==month]['TEST_QUANTITY'].sum()
                pass_value = month_wise.loc[month_wise['MONTH']==month]['PASS_QUANTITY'].sum()
                fail_value = month_wise.loc[month_wise['MONTH']==month]['REJECT_QUANTITY'].sum()
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
            else:
                specific = design_wise.loc[design_wise['MONTH']==month]
                tested_value = specific.loc[specific['DESIGN'] == design_no]['TEST_QUANTITY'].sum()
                pass_value = specific.loc[specific['DESIGN'] == design_no]['PASS_QUANTITY'].sum()
                fail_value = specific.loc[specific['DESIGN'] == design_no]['REJECT_QUANTITY'].sum()
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
    else:
        if month is None or len(month) == 0:
            if design_no == 'all_values':
                tested_value = total['TEST_QUANTITY'].loc[product]
                pass_value = total['PASS_QUANTITY'].loc[product]
                fail_value = total['REJECT_QUANTITY'].loc[product]
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
            else:
                specific = design_wise.loc[design_wise['PRODUCT'] == product]
                tested_value = specific.loc[specific['DESIGN'] == design_no]['TEST_QUANTITY'].sum()
                pass_value = specific.loc[specific['DESIGN'] == design_no]['PASS_QUANTITY'].sum()
                fail_value = specific.loc[specific['DESIGN'] == design_no]['REJECT_QUANTITY'].sum()
                fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                # return tested_value, pass_value, fail_value, fty_value
        else:
            month_check = month_wise.loc[month_wise['MONTH']==month]
            if product in month_check["TEST_QUANTITY"]:
                if design_no == 'all_values':
                    a = month_wise.loc[month_wise['MONTH']==month]
                    # print(a)
                    tested_value = a['TEST_QUANTITY'].loc[product].sum()
                    pass_value = a['PASS_QUANTITY'].loc[product].sum()
                    fail_value = a['REJECT_QUANTITY'].loc[product].sum()
                    fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                    dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                    # return tested_value, pass_value, fail_value, fty_value
                else:
                    specific = design_wise.loc[design_wise['PRODUCT'] == product]
                    filtered = specific.loc[specific['MONTH']==month]
                    tested_value = filtered.loc[filtered['DESIGN'] == design_no]['TEST_QUANTITY'].sum()
                    pass_value = filtered.loc[filtered['DESIGN'] == design_no]['PASS_QUANTITY'].sum()
                    fail_value = filtered.loc[filtered['DESIGN'] == design_no]['REJECT_QUANTITY'].sum()
                    fty_value = str(round(((pass_value / tested_value) * 100), 1)) + '%'
                    dpt_value = str(int(round(((fail_value / tested_value) * 1000), 0)))
                    # return tested_value, pass_value, fail_value, fty_value
            else:
                tested_value = 0
                pass_value = 0
                fail_value = 0
                fty_value = 0
                dpt_value = 0

    return tested_value, pass_value, fail_value, fty_value, dpt_value


@callback(Output(component_id='Design', component_property='options'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks')],
          [State('clear-cache-store', 'data')
           ])
def dropdown_part_no_summary(product, month, n_clicks, store_data):
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    filtered_df = sql_data("card_part")
    if product is None:
        if month is None or len(month) == 0:
            d = filtered_df
        else:
            d = filtered_df.loc[filtered_df['MONTH']==month]
    else:
        specific = filtered_df.loc[filtered_df['PRODUCT'] == product]
        if month is None or len(month) == 0:
            d = specific
        else:
            d = specific.loc[specific['MONTH']==month]
    return [{"label": j, "value": j} for j in d['DESIGN'].unique()]+[{'label': 'Select all', 'value': 'all_values'}]


@callback(Output(component_id='Product_all', component_property='options'),
          [Input(component_id='Month_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks')],
          [State('clear-cache-store', 'data')
           ])
def dropdown_product_all(month, design, n_clicks, store_data):
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    # modules = ['2KW SMR', '3KW SMR', '4KW SMR', 'SMR_2KW_SOLAR', 'SMR_3KW_SOLAR', 'CHARGER', 'M1000', 'M2000',
    # 'WCBMS', 'SMR_1.1KW', 'CHARGER_1.1KW']
    df_raw1 = sql_data("card")
    # filtered_df = df_raw2.loc[df_raw2['PRODUCT_NAME'].isin(modules)]
    filtered_df = df_raw1

    if month is None or len(month) == 0:
        if design == "all_values":
            d = filtered_df
        else:
            d = filtered_df.loc[filtered_df['DESIGN'] == design]
    else:
        specific = filtered_df.loc[filtered_df['MONTH']==month]
        if design == "all_values":
            d = specific
        else:
            d = specific.loc[specific['DESIGN'] == design]

    return [{"label": j, "value": j} for j in d['PRODUCT'].unique()]


@callback(Output(component_id='Month_all', component_property='options'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='month-slider', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks')],
          [State('clear-cache-store', 'data')
           ])
def dropdown_month(product, selected_months, n_clicks, store_data):
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    filtered_df = sql_data("card")
    if product is None:
        d = filtered_df
    else:
        specific = filtered_df.loc[filtered_df['PRODUCT']== product]
        d = specific
    start_index, end_index = selected_months
    # print(start_index, end_index)
    final_month = month_data(d)
    selected_data = final_month[start_index:end_index + 1]
    final_month_list = selected_data

    return [{"label": j, "value": j} for j in final_month_list]



@callback(Output(component_id='Assembly_component_bar', component_property='figure'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks'),
           Input(component_id='month-slider', component_property='value'),],
          [State('clear-cache-store', 'data')])
def bar_chart_fc_comp(product, design, month, n_clicks, selected_months, store_data):
    fig2=None
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    da = sql_data("raw")
    # final_month_list = month_data(da)
    # filtered_df = da[da['MONTH'].isin(final_month_list[-13:])]
    filtered_df = da.copy()

    final_month_list = month_data(filtered_df)

    start_index, end_index = selected_months
    filtered_df = filtered_df.loc[(filtered_df["MONTH"].isin(final_month_list[start_index:end_index+1]))]

    # print(filtered_df)

    # filtered_df=sql_data("card_design")
    try:
        if product is None:
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = filtered_df
                    final_month_list=month_data(d)
                    fc_title = f'FAULT CATEGORY COMPARISON ({final_month_list[0]} - {final_month_list[-1]})'

                else:
                    d = filtered_df.loc[filtered_df['DESIGN'] == design]
                    final_month_list = month_data(d)
                    fc_title = f'FAULT CATEGORY COMPARISON ({final_month_list[0]} - {final_month_list[-1]})'

            else:
                if design == 'all_values':
                    d = filtered_df.loc[filtered_df['MONTH']==month]
                    fc_title = f'FAULT CATEGORY COMPARISON ({month})'
                else:
                    a = filtered_df.loc[filtered_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]
                    fc_title = f'FAULT CATEGORY COMPARISON ({month})'
        else:
            specific_df = filtered_df.loc[filtered_df['PRODUCT_NAME'] == product]
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = specific_df
                    final_month_list = month_data(d)
                    fc_title = f'FAULT CATEGORY COMPARISON ({final_month_list[0]} - {final_month_list[-1]})'

                else:
                    d = specific_df.loc[specific_df['DESIGN'] == design]
                    final_month_list = month_data(d)
                    fc_title = f'FAULT CATEGORY COMPARISON ({final_month_list[0]} - {final_month_list[-1]})'

            else:
                if design == 'all_values':
                    d = specific_df.loc[specific_df['MONTH']==month]
                    fc_title = f'FAULT CATEGORY COMPARISON ({month})'

                else:
                    a = specific_df.loc[specific_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]
                    fc_title = f'FAULT CATEGORY COMPARISON ({month})'
        d['FAULT_CATEGORY'] = d['FAULT_CATEGORY'].str.upper()
        print(d)
        final_month_list = month_data(d)

        d['MONTH'] = pd.Categorical(d['MONTH'], categories=final_month_list, ordered=True)
        # print(d["FAULT_CATEGORY"].unique())
        # Filter the data for 'ASSEMBLY ISSUE' and 'COMPONENT FAIL ISSUE'
        # print("Dataframe:\n" , d)
        # print("Dataframe:\n", d["FAULT_CATEGORY"].unique())
        filtered_data = d[d['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT","LEAD CUT ISSUE","OPERATOR FAULT", "COATING ISSUE", "COMP. DMG/MISS", "WRONG MOUNT", "REVERSE POLARITY" ])]

        filtered_data1 = d[d['FAULT_CATEGORY'].isin(["COMP. FAIL", "CC ISSUE"])]

        # Step 1: Calculate the total quantity of products in each respective month
        total_quantity_per_month = d.groupby('MONTH')['PRODUCT_NAME'].count()
        # print(total_quantity_per_month)

        # Step 2: Calculate the occurrences of each category for table1
        table1 = filtered_data1.groupby(['MONTH', 'FAULT_CATEGORY'])['PRODUCT_NAME'].count().unstack(fill_value=0)
        # print(table1)
        table1['COMPO. FAIL'] = table1.sum(axis=1)
        # print(table1)

        # Step 3: Group the data month-wise and count the occurrences of each category for table
        table = filtered_data.groupby(['MONTH', 'FAULT_CATEGORY'])['PRODUCT_NAME'].count().unstack(fill_value=0)
        table['ASSEMBLY ISSUE'] = table.sum(axis=1)
        # Step 4: Merge the two tables
        final_table = pd.merge(table, table1, how="left", on="MONTH")

        # Step 5: Calculate the percentage of COMP. FAIL issues out of the total quantity of products in each respective month
        final_table['ASSEMBLY ISSUE %'] = round(((table["ASSEMBLY ISSUE"] / total_quantity_per_month) * 100), 0)
        final_table['COMP. FAIL %'] = round(((table1["COMPO. FAIL"] / total_quantity_per_month) * 100), 0)

        final_table = final_table.replace(np.nan, 0)

        # final_table.set_index('MONTH',inplace=True)
        # final_table = final_table.reindex(final_month_list, fill_value=0)
        # final_table.reset_index(inplace =True)
        # print(final_table)
        #
        # start_index, end_index = selected_months
        #
        #
        #
        # # print(start_index, end_index)
        # selected_data = final_table.iloc[start_index:end_index + 1]
        # Step 6: Transpose the DataFrame
        transposed_df = final_table.T
        # print(type(transposed_df))
        # print(transposed_df)

        transposed_df = transposed_df.tail(2)
        # print(transposed_df)


        #
        #
        # assembly_x = np.arange(len(transposed_df.columns))
        # assembly_y = transposed_df.loc['ASSEMBLY ISSUE %'].values
        # # assembly_trendline_coeffs = np.polyfit(assembly_x, assembly_y, 1)
        # #
        # # # Calculate the trendline coefficients for 'COMP. FAIL %'
        # comp_fail_x = np.arange(len(transposed_df.columns))
        # comp_fail_y = transposed_df.loc['COMP. FAIL %'].values
        #
        # mask = ~np.isnan(assembly_y) & ~np.isinf(assembly_y)
        # assembly_x_clean = assembly_x[mask]
        # assembly_y_clean = assembly_y[mask]
        # mask = ~np.isnan(comp_fail_y) & ~np.isinf(comp_fail_y)
        # comp_fail_x_clean = comp_fail_x[mask]
        # comp_fail_y_clean = comp_fail_y[mask]
        #
        # # comp_fail_trendline_coeffs = np.polyfit(comp_fail_x, comp_fail_y, 1)
        # if np.any(assembly_x_clean == 0):
        #     assembly_trendline_coeffs = None
        # else:
        #     # Perform linear fit
        #     assembly_trendline_coeffs = np.polyfit(assembly_x_clean, assembly_y_clean, 1)
        #
        # # Check for zero division for 'COMP. FAIL %'
        # if np.any(comp_fail_x_clean == 0):
        #     comp_fail_trendline_coeffs = None
        # else:
        #     # Perform linear fit
        #     comp_fail_trendline_coeffs = np.polyfit(comp_fail_x_clean, comp_fail_y_clean, 1)
        #
        # # Create the trendlines if the coefficients are available
        # if assembly_trendline_coeffs is not None:
        #     assembly_trendline = assembly_trendline_coeffs[0] * assembly_x + assembly_trendline_coeffs[1]
        # else:
        #     assembly_trendline = None
        #
        # if comp_fail_trendline_coeffs is not None:
        #     comp_fail_trendline = comp_fail_trendline_coeffs[0] * comp_fail_x + comp_fail_trendline_coeffs[1]
        # else:
        #     comp_fail_trendline = None
        # #
        # # # Create the trendlines
        # # assembly_trendline = assembly_trendline_coeffs[0] * assembly_x + assembly_trendline_coeffs[1]
        # # comp_fail_trendline = comp_fail_trendline_coeffs[0] * comp_fail_x + comp_fail_trendline_coeffs[1]

        fig2 = go.Figure()

        # Add a bar for 'ASSEMBLY ISSUE %'
        fig2.add_trace(go.Bar(
            x=transposed_df.columns,
            y=transposed_df.loc['ASSEMBLY ISSUE %'],
            name='ASSEMBLY ISSUE %',
            marker_color='#104E8B',
            text=[f'{val:.0f}%' for val in transposed_df.loc['ASSEMBLY ISSUE %']],
            textposition='inside',textfont=dict(size=12,family='Arial Black',color ='white'
                                                 ), showlegend=True, textangle=270))

        # Add a bar for 'COMP. FAIL %'
        fig2.add_trace(go.Bar(
            x=transposed_df.columns,
            y=transposed_df.loc['COMP. FAIL %'],
            name='COMP. FAIL %',
            marker_color='#EE2C2C',
            text=[f'{val:.0f}%' for val in transposed_df.loc['COMP. FAIL %']],
            textposition='inside',textfont=dict(size=12,family='Arial Black', color='white'
                                                 ),showlegend=True,  textangle=270))

       # Add the trendlines to the plot
       #  fig2.add_trace(go.Scatter(
       #      x=transposed_df.columns,
       #      y=assembly_trendline,
       #      mode='lines',
       #      name='ASSEMBLY ISSUE % Trendline',
       #      line=dict(color='blue', width=3, dash='dash'),
       #      showlegend=False
       #  ))
       #
       #  fig2.add_trace(go.Scatter(
       #      x=transposed_df.columns,
       #      y=comp_fail_trendline,
       #      mode='lines',
       #      name='COMP. FAIL % Trendline',
       #      line=dict(color='red', width=3, dash='dash'),
       #      showlegend=False
       #  ))
       #  final_month_list = month_data(d)
       #  month_string = f'{final_month_list[-13]}-{final_month_list[-1]}'
       #  month_string = month_string.replace(',', "'")
        # Customize the layout
        # if month is None or len(month) == 0:
        #     fc_title=f'FAULT CATEGORY COMPARISON ({final_month_list[start_index]} - {final_month_list[end_index]})'
        # else:
        #     fc_title=f'FAULT CATEGORY COMPARISON ({month})'
        fig2.update_layout(
        title=dict(text=fc_title, font=dict(size=18, family='Arial Black')),
        xaxis_title=dict(text='Months', font=dict(size=12, family='Arial Black')),
        yaxis_title=dict(text='Fault Percentage', font=dict(size=14, family='Arial Black')),
        showlegend=True,
        legend=dict(
            orientation='h',  # Horizontal orientation for the legend
            x=0.22,  # Position the legend at the center of the graph horizontally
            y=1.12,  # Position the legend slightly above the graph
        ))
        fig2.update_xaxes(tickfont_family='Arial Black', tickangle=315)
        fig2.update_yaxes(tickfont_family='Arial Black')




    except Exception as e:
        print(f"Error: {e}")

    return fig2


@callback(Output(component_id='DPT_comparison', component_property='figure'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks'),
           Input(component_id='month-slider', component_property='value'), ],
          [State('clear-cache-store', 'data')
           ])
def bar_chart_dpt_comp(product, design, month, n_clicks, selected_months, store_data):
    fig=None
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    da = sql_data("card_design")
    # final_month_list = month_data(da)
    # filtered_df = da[da['MONTH'].isin(final_month_list[-13:])]
    filtered_df =da

    try:
        if product is None:
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = filtered_df
                else:
                    d = filtered_df.loc[filtered_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = filtered_df.loc[filtered_df['MONTH']==month]
                else:
                    a = filtered_df.loc[filtered_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]
        else:
            specific_df = filtered_df.loc[filtered_df['PRODUCT'] == product]
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = specific_df
                else:
                    d = specific_df.loc[specific_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = specific_df.loc[specific_df['MONTH']==month]
                else:
                    a = specific_df.loc[specific_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]

        final_month_list = month_data(d)

        month_string = f'{final_month_list[0]}-{final_month_list[-1]}'
        month_string = month_string.replace(',', "'")

        # print(d.columns)
        # print(d)
        # custom_month_order = ['APR,22', 'MAY,22', 'JUN,22', 'JUL,22', 'AUG,22', 'SEP,22', 'OCT,22', 'NOV,22', 'DEC,22',
        #                       'JAN,23', 'FEB,23', 'MAR,23', 'APR,23', 'MAY,23', 'JUN,23']

        # Convert the 'MONTH' column to a categorical variable with the custom order
        d['MONTH'] = pd.Categorical(d['MONTH'], categories=final_month_list, ordered=True)
        # d = d[d["MONTH"].isin(final_month_list[-13:])]
        # print(d["MONTH"].unique())
        d = d.groupby(['MONTH'])[['TEST_QUANTITY', 'REJECT_QUANTITY']].sum().reset_index()

        d['DPT'] = round((d['REJECT_QUANTITY'] / d['TEST_QUANTITY']) * 1000, 0)

        # print(d)
        start_index, end_index = selected_months
        selected_data = d.iloc[start_index:end_index + 1]

        # print(d["DESIGN"].unique())
        # Create the bar chart using plotly.graph_objects
        if month is None or len(month) == 0:
            dpt_title = f'DPT COMPARISON MONTH-WISE ({final_month_list[start_index]} - {final_month_list[end_index]})'
        else:
            dpt_title=f'DESIGN_WISE FAULT PERCENTAGE ({month})'
        # print(selected_data)
        fig = go.Figure(
            data=go.Bar(x=selected_data['MONTH'], y=selected_data['DPT'],text=[f'{val:.0f}' for val in selected_data['DPT']],
                        textposition='inside', textfont=dict(size=10, family='Arial Black'), marker_color='#104E8B', textangle=270),
        )
        # fig.update_layout()

        # Update the layout
        fig.update_layout(
            title=dict(text=dpt_title, font=dict(size=18, family='Arial Black')),
            xaxis_title=dict(text='Months', font=dict(size=12, family='Arial Black')),
            yaxis_title=dict(text='DPT (Defects Per Thousand)', font=dict(size=14, family='Arial Black')),
            showlegend=False
        )
        # fig.update_traces(text=d['DPT'])
        fig.update_xaxes(tickfont_family='Arial Black', tickangle=315)
        fig.update_yaxes(tickfont_family='Arial Black')

        coefficients = np.polyfit(selected_data.index, selected_data['DPT'], 1)
        trendline_values = np.polyval(coefficients, selected_data.index)
        fig.add_trace(
            go.Scatter(x=selected_data['MONTH'], y=trendline_values, mode='lines', line=dict(dash='dash', width=3),
                       name='Trendline'))
    except Exception as e:
        print(f"Error: {e}")

    return fig


@callback(Output(component_id='Summary_highlight', component_property='children'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks')],
          [State('clear-cache-store', 'data')
           ])
def summary_table_highlights_page4(product, design, month, n_clicks, store_data):
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    filtered = sql_data("raw")
    # filtered = filtered[filtered["MONTH"] == month_data(filtered)]
    # print(filtered)
    filtered1= sql_data("card")
    final_month=month_data(filtered1)
    final_month=final_month[-13:]
    # filtered = filtered[filtered["MONTH"] == month_data(filtered)]
    # filtered1 = filtered1[filtered1["MONTH"] == month_data(filtered1)]
    try:
        if product is None:
            if month is None or len(month) == 0:
                if design == 'all_values':
                    a = table_summary_select_all(filtered, filtered1)
                    # print(a)
                else:
                    filtered=filtered[filtered["MONTH"].isin(month_data(filtered1))]
                    # print("entered")
                    # filtered1=filtered1[filtered1["MONTH"]==month_data(filtered1)[-7:-1]]
                    b = filtered.loc[filtered['DESIGN'] == design]
                    c = filtered1.loc[filtered1['DESIGN'] == design]
                    # print(design)
                    a = table_summary_highlights(b,c)

            else:
                if design == 'all_values':
                    index=final_month.index(month)
                    months = final_month[0:index + 1]
                    b = filtered.loc[filtered['MONTH'].isin(months)]
                    c= filtered1.loc[filtered1['MONTH'].isin(months)]
                    a = table_summary_select_all(b,c)
                else:
                    filtered = filtered[filtered["MONTH"].isin(month_data(filtered1))]
                    index=final_month.index(month)
                    months = final_month[0:index + 1]
                    b = filtered.loc[filtered['MONTH'].isin(months)]
                    c = b.loc[b['DESIGN'] == design]
                    d = filtered1.loc[filtered1['MONTH'].isin(months)]
                    e = d.loc[d['DESIGN'] == design]
                    a = table_summary_highlights(c,e)
        else:
            specific_df = filtered.loc[filtered['PRODUCT_NAME'] == product]
            specific_df1 = filtered1.loc[filtered1['PRODUCT'] == product]
            if month is None or len(month) == 0:
                if design == 'all_values':
                    a = table_summary_product(specific_df, specific_df1)
                else:
                    b = specific_df.loc[specific_df['DESIGN'] == design]
                    c = specific_df1.loc[specific_df1['DESIGN'] == design]
                    a = table_summary_product(b, c)
            else:
                if design == 'all_values':
                    b = specific_df.loc[specific_df['MONTH']==month]
                    c = specific_df1.loc[specific_df1['MONTH']==month]
                    a = table_summary_product(b,c)
                else:
                    b = specific_df.loc[specific_df['MONTH']==month]
                    c = b.loc[b['DESIGN'] == design]
                    d = specific_df1.loc[specific_df1['MONTH']==month]
                    e = d.loc[d['DESIGN'] == design]
                    a = table_summary_product(c,e)



    except:

        return 0

    # a["Remarks"] = a["Remarks"].apply(render_Cell)
    return html.Div([dash_table.DataTable(data=a.to_dict('records'),
                                          columns=[
                                              {'name': j, 'id': j} for j in a.columns],
                                          editable=False,
                                          # filter_action="native",
                                          sort_action="native",
                                          sort_mode='multi',
                                          fixed_rows={'headers': True},
                                          merge_duplicate_headers=True,
                                          # header=headers,
                                          style_header={'backgroundColor': "rgb(50, 50, 50)",
                                                        'color': 'white',
                                                        'fontWeight': 'bold',
                                                        'textAlign': 'center'},
                                          style_cell_conditional=[{'if': {'column_index': 0},
                                                                   'fontWeight': 'bold'},
                                                                  {'if': {'column_id': 'PRODUCT'},
                                                                   'width': '10%', 'fontWeight': 'bold',
                                                        'textAlign': 'center'},
                                                                  {'if': {'column_id': 'Design'},
                                                                   'width': '7%', 'fontWeight': 'bold',
                                                                   'textAlign': 'center'},
                                                                  {'if': {'column_id': 'Total'},
                                                                   'width': '1.5%'},
                                                                  {'if': {'column_id': '%age'},
                                                                   'width': '1.5%'},
                                                                  {'if': {'column_id': 'ATE'},
                                                                   'width': '2%'},
                                                                  {'if': {'column_id': 'Burn-in'},
                                                                   'width': '2.2%'},
                                                                  {'if': {'column_id': 'Initial'},
                                                                   'width': '3.2%'},
                                                                  {'if': {'column_id': 'Pre-initial'},
                                                                   'width': '3.2%'},
                                                                  {'if': {'column_id': 'Remarks'},
                                                                   'width': '30%',  'textAlign': 'left'},
                                                                  {'if': {'column_id': 'Major Faults'},
                                                                   'width': '30%', 'textAlign': 'left'},
                                                                ],

                                          style_cell={'textAlign': 'center', 'height': 'auto',
                                                      'backgroundColor': 'white', 'color': 'black',
                                                      'width': '6%',
                                                      'maxWidth': '100px',"whiteSpace": "pre-line"},
                                          style_table={
                                              # 'maxHeight': "40vh",
                                              'height': '10%',  # Set the height to 'auto' to display the table in full
                                              'overflowY': 'auto',
                                              # Add vertical scrolling if the table height exceeds the screen height
                                          },
                                          # fill_width=False
                                          ),
                     html.Hr()
                     ])




@callback(Output(component_id='Sunburst', component_property='figure'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks'),
           Input(component_id='month-slider', component_property='value'),],
          [State('clear-cache-store', 'data')
           ])
def sunburst_chart(product, design, month, n_clicks, selected_months, store_data):
    fig=None
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    da = sql_data("raw")
    # card=sql_data('card')
    # # final_month_list = month_data(da)
    # # filtered_df = da[da['MONTH'].isin(final_month_list[-13:])]
    filtered_df =da

    # product = "AC EV_171"
    # month = "JUL,23"
    # part_no = "HE317171-35.12"

    try:
        if product is None:
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = filtered_df
                else:
                    d = filtered_df.loc[filtered_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = filtered_df.loc[filtered_df['MONTH']==month]
                else:
                    a = filtered_df.loc[filtered_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]
        else:
            specific_df = filtered_df.loc[filtered_df['PRODUCT_NAME'] == product]
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = specific_df
                else:
                    d = specific_df.loc[specific_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = specific_df.loc[specific_df['MONTH']==month]
                else:
                    a = specific_df.loc[specific_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]

        final_month_list = month_data(d)

        # Convert the 'MONTH' column to a categorical variable with the custom order
        d['MONTH'] = pd.Categorical(d['MONTH'], categories=final_month_list, ordered=True)
        # d = d[d["MONTH"].isin(final_month_list[-13:])]
        # print(d["MONTH"].unique())
        colors_df = {'F1': '#3CB371', 'F2': '#BF3EFF'}
        d['Area'] = ''
        f2 = [
            'M2000', 'WCBMS', 'DCIO_F2',
            'SMR_1.1KW', 'CHARGER_1.1KW', 'CHARGER', 'SMR_2KW_SOLAR', '3KW SMR', '2KW SMR', '4KW SMR', 'SMR_3KW_SOLAR',
            '2KW SUN MOBILITY', 'SMR_RETROFIT', 'M1000'
        ]
        d.loc[d['PRODUCT_NAME'].isin(f2), 'Area'] = 'F2'
        d.loc[d['Area'] == '', 'Area'] = 'F1'


        assembly = ['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS", "WRONG MOUNT", "REVERSE POLARITY",
                    "LEAD CUT ISSUE", "OPERATOR FAULT", "COATING ISSUE"]
        component = ["COMP. FAIL", "CC ISSUE"]
        # d['CATEGORY'] = ''
        d.loc[d['FAULT_CATEGORY'].isin(assembly), 'CATEGORY'] = 'Assembly'
        d.loc[d['FAULT_CATEGORY'].isin(component), 'CATEGORY'] = 'Component'
        d['CATEGORY'] = d['CATEGORY'].fillna('Others')
        #
        # if (d['CATEGORY'] != 'Assembly').any():
        #     if (d['CATEGORY'] != 'Component').any():
        #         d['CATEGORY'] = 'Others'
        # d.to_excel("ASDFGHJKL.xlsx")
        asse_q = 0
        comp_q = 0
        for i in d['FAULT_CATEGORY']:
            if i in assembly:
                asse_q += 1
            elif i in component:
                comp_q += 1
        total_faults = len(d)
        asse_per = asse_q / total_faults * 100
        comp_per = comp_q / total_faults * 100
        d['Assembly'] = asse_per
        d['Component'] = comp_per
        # print(d['DESIGN'].unique())

        #selected_months
        # selected_months
        start_index, end_index = selected_months
        # print(start_index, end_index)
        final_month=month_data(d)
        # print(d["MONTH"].unique())
        selected_data = d.copy()

        d = selected_data[selected_data['MONTH'].isin(final_month[start_index:end_index+1])]
        # print(d)
        fig = px.sunburst(
            d,
            path=['Area', 'DESIGN', 'PRODUCT_NAME', 'CATEGORY', 'FAULT_CATEGORY', 'KEY_COMPONENT'],
            custom_data=['Area'],
            color='Area',
            color_discrete_map=colors_df,
            maxdepth=2,
        )

        if month is None or len(month) == 0:
            cw_title=f'CATEGORY WISE FAULT ({final_month_list[start_index]} - {final_month_list[end_index]})'
        else:
            cw_title=f'CATEGORY WISE FAULT ({month})'

        fig.update_traces(textinfo='label+value', textfont_size=14, insidetextfont_color='white')
        fig.update_layout(margin=dict(b=10), plot_bgcolor="white", paper_bgcolor="white",
                          font_color="black")
        fig.update_layout(title=cw_title,font=dict(size=12, family='Arial Black'), title_pad_b=0, title_y=0.96)
        fig.update_traces(hovertemplate="Label:%{label} <br>Count:%{value}<br>Parent:%{parent}")

    except Exception as e:
        print(f"Error: {e}")

    return fig




@callback(Output(component_id='Overall_graph', component_property='figure'),
          [Input(component_id='Product_all', component_property='value'),
           Input(component_id='Design', component_property='value'),
           Input(component_id='Month_all', component_property='value'),
           Input(component_id='clear-cache-button', component_property='n_clicks'),
           Input(component_id='month-slider', component_property='value'), ],
          [State('clear-cache-store', 'data')
           ])
def bar_chart_overall(product, design, month, n_clicks, selected_months, store_data):
    fig=None
    if n_clicks > store_data:
        clear_cache()
        store_data = n_clicks
    filtered_df = sql_data("card")

    try:
        if product is None:
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = filtered_df
                else:
                    d = filtered_df.loc[filtered_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = filtered_df.loc[filtered_df['MONTH']==month]
                else:
                    a = filtered_df.loc[filtered_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]
        else:
            specific_df = filtered_df.loc[filtered_df['PRODUCT'] == product]
            if month is None or len(month) == 0:
                if design == 'all_values':
                    d = specific_df
                else:
                    d = specific_df.loc[specific_df['DESIGN'] == design]
            else:
                if design == 'all_values':
                    d = specific_df.loc[specific_df['MONTH']==month]
                else:
                    a = specific_df.loc[specific_df['MONTH']==month]
                    d = a.loc[a['DESIGN'] == design]

        # month_list = month_data(d)
        if month is None or len(month) == 0:
            final_month_list = month_data(d)
            d['MONTH'] = pd.Categorical(d['MONTH'], categories=final_month_list, ordered=True)
            start_index, end_index = selected_months
            print(start_index, end_index)

            selected_data = d.copy()

            d = selected_data[selected_data['MONTH'].isin(final_month_list[start_index:end_index + 1])]
            b = f'<b>DESIGN_WISE FAULT PERCENTAGE ({final_month_list[start_index]} - {final_month_list[end_index]})<b>'
        else:
            b = f'<b>DESIGN_WISE FAULT PERCENTAGE ({month})<b>'

        df = d
        dct__old = df[df['DESIGN'] == 'DCT_OLD']
        dct__new = df[df['DESIGN'] == 'DCT_NEW']
        f2__old = df[df['DESIGN'] == 'F2_OLD']
        f2__new = df[df['DESIGN'] == 'F2_NEW']
        ev = df[df['DESIGN'] == 'EVSE']
        tq = (dct__old['REJECT_QUANTITY'].sum()) + (dct__new['REJECT_QUANTITY'].sum()) + (
            f2__old['REJECT_QUANTITY'].sum()) + (f2__new['REJECT_QUANTITY'].sum()) + (ev['REJECT_QUANTITY'].sum())
        fault_perc_dct_old = round((dct__old['REJECT_QUANTITY'].sum() / tq) * 100, 0)
        fault_perc_dct_new = round((dct__new['REJECT_QUANTITY'].sum() / tq) * 100, 0)
        fault_perc_f2_old = round((f2__old['REJECT_QUANTITY'].sum() / tq) * 100, 0)
        fault_perc_f2_new = round((f2__new['REJECT_QUANTITY'].sum() / tq) * 100, 0)
        fault_perc_ev = round((ev['REJECT_QUANTITY'].sum() /tq) * 100, 0)

        data = {'DESIGN': ['DCT_OLD', 'DCT_NEW', 'F2_OLD', 'F2_NEW', 'EVSE'],
                'Value': [fault_perc_dct_old, fault_perc_dct_new, fault_perc_f2_old, fault_perc_f2_new, fault_perc_ev]}

        df = pd.DataFrame(data)
        non_zer_df=df[df['Value']!=0]

        # design_title=f'DESIGN_WISE FAULT PERCENTAGE {}'

        fig = px.pie(non_zer_df, values='Value', names='DESIGN', title=b,
                     hole=0)

        # Customize the marker to add white edges
        fig.update_traces(marker=dict(line=dict(color='white', width=3)),
                          textinfo='percent+label',textfont=dict(size=14))
    # textposition='outside')

    except Exception as e:
        print(f"Error: {e}")

    return fig