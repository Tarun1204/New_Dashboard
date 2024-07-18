import plotly.express as px
import pandas as pd
import sqlite3
import plotly.graph_objects as go

conn = sqlite3.connect("ALL_FAULTS_INCLUDED.sqlite3")
query_card = 'SELECT * FROM CARD;'
card = pd.read_sql_query(query_card, conn)
query_raw = 'SELECT * FROM RAW;'
raw = pd.read_sql_query(query_raw, conn)
card=card[card["DESIGN"]=="EVSE"]
# card=card[card["DESIGN"]=="EVSE"]
raw=raw[raw["DESIGN"]=="EVSE"]
# raw=raw[raw["DESIGN"]=="EVSE"]
list_of_month=card["MONTH"].unique()


def month_data(df_card_data):
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    df_month = df_card_data['MONTH'].unique()
    # print(df_month)
    sorted(df_month, key=lambda x: months.index(x.split(",")[0]))
    month_order = sorted(df_month, key=lambda x: (int(x.split(",")[1]), months.index(x.split(",")[0])))

    return month_order


month_order = month_data(card)
df_card = card
df_raw_all = raw
month=df_card["MONTH"].unique()
df_raw = df_raw_all[df_raw_all["MONTH"] == "AUG,22"]
print(df_raw["PRODUCT_NAME"].unique())
# print(df_raw["PRODUCT_NAME"].unique())
# print(df_card["PRODUCT"].unique())
# print(month)

# initialize dataframes
df_crnt_total, df_previous_month = pd.DataFrame(), pd.DataFrame()
df_summ_merged, df_summ_merged_pilot = pd.DataFrame(), pd.DataFrame()
df_summary, df_summary_pilot = pd.DataFrame(), pd.DataFrame()

# initialize dictionaries
df_final_total, df_final_summary, df_final_comp = {}, {}, {}
df_final_remarks, partcode_list, list_of_month_all = {}, {}, {}
df_final_total_pilot, df_final_summary_pilot = {}, {}
df_retest_value, df_pending_value, df_scrap_value = {}, {}, {}
df_final_comp_pilot, df_final_remarks_pilot = {}, {}
list_of_month_all_pilot, category_df = {}, {}
df_new_fault_text, df_new_fault_value = {}, {}
list_of_month_fst_page = df_card['MONTH'].unique()

import re
def alphaNumOrder(string):
    return ''.join([format(int(x), '05d') if x.isdigit()
                    else x for x in re.split(r'(\d+)', string)])

df_raw["KEY_COMPONENT"] = df_raw['KEY_COMPONENT'].str.upper().str.replace('&', ',').str.replace(' &',
                                                                                                ',').str.replace \
    ('& ', ',').str.replace(', ', ',').str.replace(' , ', ',').str.replace(' ,', ',').str.replace('and',
                                                                                                  ',').str.replace(
    ' ', '_')
df_raw['KEY_COMPONENT'] = df_raw['KEY_COMPONENT'].str.strip(',').str.strip('_')

for m in df_raw["PRODUCT_NAME"].unique():
    print(m)
    df10 = df_raw[df_raw["PRODUCT_NAME"] == m]
    # df_pending_date = df10[['FAULT_OBSERVED', 'DATE', 'FAULT_CATEGORY']]
    # df_pending_date = df_pending_date[df_pending_date["FAULT_CATEGORY"] == "PENDING"]
    df = df10.groupby(["FAULT_OBSERVED"], as_index=False).count()
    df11 = df10.pivot_table(columns='FAULT_CATEGORY', index="FAULT_OBSERVED", values='KEY_COMPONENT',
                            aggfunc=lambda x: str(x.count()) + '\n ' + "(" + ', '.join(
                                str(v) + ' : ' + str(x.value_counts()[v]) if (
                                        str(v) != "NOT_FOUND" and str(v) != "NFF") else "" for v in
                                x.unique() if x.value_counts()[v] < 30) + ")").fillna('').reset_index()

    if "PENDING" in df11.columns:
        df11["PENDING"] = df11["PENDING"].str.strip("()").str.strip()
        df_pending_count = df11.copy()
        df11["PENDING"].fillna("", inplace=True)
        df_pending_count["PENDING"].replace("", "0", inplace=True)
        df_pending_count["PENDING"] = pd.to_numeric(df_pending_count["PENDING"], errors='coerce').astype(int)
        df_pending_value[m] = df_pending_count["PENDING"].sum()
        # print(df_pending_value[m])

    if "RETEST" in df11.columns:
        df11["RETEST"] = df11["RETEST"].str.strip("()").str.strip()
        df_retest_count = df11.copy()
        df11["RETEST"].fillna("", inplace=True)
        df_retest_count["RETEST"].replace("", "0", inplace=True)
        df_retest_count["RETEST"] = pd.to_numeric(df_retest_count["RETEST"], errors='coerce').astype(int)
        df_retest_value[m] = df_retest_count["RETEST"].sum()
        # print(df_retest_value[m])

    df_only_key_component = df10.pivot_table(columns='FAULT_CATEGORY', index="FAULT_OBSERVED",
                                             values='KEY_COMPONENT', aggfunc=lambda x: ', '.join(
            str(v) + ' : ' + str(x.value_counts()[v]) for v in x.unique() if x.value_counts()[v] < 30)).fillna(
        '').reset_index()

    if "SCRAP" in df_only_key_component.columns:
        df_scrap_count = df_only_key_component.copy()
        df_scrap_count["SCRAP"].fillna("", inplace=True)
        df_scrap_count = df_scrap_count[df_scrap_count["SCRAP"] != ""]
        df_scrap_count = df_scrap_count["SCRAP"].str.cat(sep=", ")
        df_scrap_value[m] = df_scrap_count
        # print(df_scrap_value[m])
        # print(df_scrap_value[m])

    # print(df11)

    df11 = df11.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                "COMPONENT MISSING": "Miss",
                                "Component faulty": "faulty",
                                "FAULT_OBSERVED": "Faults",
                                "REVERSE POLARITY": "Polarity",
                                "SOLDERING ISSUE": "Solder Short", "DRY SOLDER": "Dry Solder", "SCRAP": "Scrap",
                                "MAGNETICS ISSUE": "Magnetics",
                                "COMPONENT FAIL ISSUE": "Comp. Fail",
                                "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss", "PENDING": "Pending",
                                "WRONG MOUNTING": "Wrong Mount", "CC ISSUE": "CC Issue", "RETEST": "Retest"})
    df11["Faults"] = df11["Faults"].str.upper()

    df_current_mnth = df_card[df_card["PRODUCT"] == m].reset_index()
    list_of_month = df_current_mnth.MONTH.unique()
    print(m)
    if len(list_of_month) >= 2:
        print(m, "loop entered for:", m)
        df15 = df_current_mnth[df_current_mnth["MONTH"] == list_of_month[-1]]
        df_tail = pd.DataFrame(columns=["Month", "Test Quantity", "Pass Quantity", "Fail Quantity"])
        df_tail.loc[0, "Month"] = list_of_month[-1]
        tq = df15['TEST_QUANTITY'].sum()
        pq = df15['PASS_QUANTITY'].sum()
        rq = df15['REJECT_QUANTITY'].sum()

        df_tail.loc[0, "Test Quantity"] = tq
        df_tail.loc[0, "Pass Quantity"] = pq
        df_tail.loc[0, "Fail Quantity"] = rq
        print(m, df_tail)
        df

#
#         df_crnt_total["DPT**"] = round(((rq/ tq) * 1000), 0)
#         pg1 = df_crnt_total["Test Quantity"].unique()
#         tf1 = df_crnt_total["Fail Quantity"].unique()
#         fail_quant1 = df_crnt_total["Fail Quantity"].unique()
#         dpt1 = df_crnt_total["DPT**"].unique()
#         df16 = df_card[df_card["PRODUCT"] == m].reset_index()
#         list_of_month2 = df16.MONTH.unique()
#         # print(list)
#         df16 = df16[df16["MONTH"] != list_of_month2[-1]]
#         # df16.reset_index()
#         df16.at[len(df16), "Month"] = list_of_month2[0] + "-" + list_of_month2[-2]
#         df16.at[len(df16) - 1, "Test Quantity"] = df16['TEST_QUANTITY'].sum()
#         df16.at[len(df16) - 1, "Pass Quantity"] = df16['PASS_QUANTITY'].sum()
#         df16.at[len(df16) - 1, "Fail Quantity"] = df16['REJECT_QUANTITY'].sum()
#         df_previous_month = df16.tail(1)
#         df_previous_month = df_previous_month.dropna(axis=1)
#         df_previous_month["DPT**"] = round(((df16['Fail Quantity'] / df16['Test Quantity']) * 1000), 0)
#         pg2 = df_previous_month["Test Quantity"].unique()
#         tf2 = df_previous_month["Fail Quantity"].unique()
#         fail_quant2 = df_previous_month["Fail Quantity"].unique()
#         dpt2 = df_previous_month["DPT**"].unique()
#
#         category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
#         category1_df = pd.DataFrame(category)
#         category = category.reset_index()
#         category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
#         category.loc['Total'] = category.sum(axis=0)
#
#         category = category.tail(1)
#         print(category)
#
#         lis_col_names = list(category.columns.values)
#         lis_col_names_all = ["CC ISSUE", "COMP. FAIL", "COMP. DMG/MISS", "MAGNETICS ISSUE",
#                              "DRY SOLDER", "SCRAP", "SOLDER SHORT", "WRONG MOUNTING", "REVERSE POLARITY",
#                              "PENDING",
#                              "UNDER ANALYSIS", "RETEST"]
#         for i in lis_col_names_all:
#             if i not in lis_col_names:
#                 category[i] = 0
#         for i in lis_col_names_all:
#             if i not in lis_col_names:
#                 category1_df[i] = 0
#         # category.drop(["PENDING", "UNDER ANALYSIS"], axis=1, inplace=True)
#         comp_quant = category["CC ISSUE"] + category["COMP. FAIL"]
#         other_quant = category["MAGNETICS ISSUE"] + category["SCRAP"]
#         assmbly_quant = category["COMP. DMG/MISS"] + category["DRY SOLDER"] + category["WRONG MOUNTING"] + \
#                         category["REVERSE POLARITY"] + category["SOLDER SHORT"]
#         print(assmbly_quant)
#         retest_quant = category["RETEST"]
#         category = category.rename(columns={"COMPONENT DAMAGE": "Dmg",
#                                             "COMPONENT MISSING": "Miss",
#                                             "Component faulty": "faulty", "PENDING": "Pending",
#                                             "CC ISSUE": "CC Issue",
#                                             "FAULT_OBSERVED": "Faults",
#                                             "REVERSE POLARITY": "Polarity",
#                                             "SOLDERING ISSUE": "Solder Short",
#                                             "COMPONENT FAIL ISSUE": "Comp. Fail",
#                                             "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss",
#                                             "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
#                                             "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest",
#                                             "SCRAP": "Scrap"})
#         # print(m)
#         df_summary["PRODUCT"] = [m]
#         df_summary["Qty1"] = pg2
#         df_summary["Fail1"] = fail_quant2
#         df_summary["DPT1"] = dpt2
#         df_summary["Qty2"] = pg1
#         df_summary["Fail2"] = fail_quant1
#         df_summary["DPT2"] = dpt1
#         df_summary["Assy."] = assmbly_quant.unique()
#         df_summary["Comp."] = comp_quant.unique()
#         df_summary["Retest"] = retest_quant.unique()
#         df_summary["Others"] = other_quant.unique()
#         df_summary["Remarks"] = ""
#         # df_summary["Remarks"] = ""
#         df_summ_merged = pd.concat([df_summ_merged, df_summary], axis=0)
#
#
#
#         partcode_list[m] = str_of_partcode
#
#         list_of_month_all[m] = list_of_month2
#         category_df[m] = category
#     elif len(list_of_month1) == 1:
#         # print(list_of_month1)
#         df15 = df15[df15["MONTH"] == list_of_month1[-1]]
#         df15.at[len(df15) + 3, "Month"] = list_of_month1[-1]
#         df15 = df15.copy()
#         index = df15[df15['Month'] == list_of_month1[-1]].index.tolist()[0]
#         df15.at[index, "Test Quantity"] = df15['TEST_QUANTITY'].sum()
#         df15.at[index, "Pass Quantity"] = df15['PASS_QUANTITY'].sum()
#         df15.at[index, "Fail Quantity"] = df15['REJECT_QUANTITY'].sum()
#         df_crnt_total = df15.tail(1)
#         # [print(df_crnt_total)]
#         df_crnt_total = df_crnt_total.dropna(axis=1)
#         df_crnt_total["DPT**"] = round(((df15['Fail Quantity'] / df15['Test Quantity']) * 1000), 0)
#         pg1 = df_crnt_total["Test Quantity"].unique()
#         tf1 = df_crnt_total["Fail Quantity"].unique()
#         fail_quant1 = df_crnt_total["Fail Quantity"].unique()
#         dpt1 = df_crnt_total["DPT**"].unique()
#         df_new = df_crnt_total.copy()
#         df18 = df_raw[df_raw["PRODUCT_NAME"] == m].reset_index()
#         df_faults2 = df18[['FAULT_OBSERVED', "DATE"]]
#         table2 = df_faults2.pivot_table(index='FAULT_OBSERVED', aggfunc='count')
#         table2 = table2.reset_index()
#         table2.rename(columns={'DATE': list_of_month1[-1]}, inplace=True)
#         table2['DPT**' + "(" + (list_of_month1[-1]) + ")"] = round((table2[list_of_month1[-1]] / pg1) * 1000, 0)
#         df_comparison = table2
#         df_comparison.at[len(df_comparison), 'FAULT_OBSERVED'] = "Grand Total"
#         df_comparison.at[len(df_comparison) - 1, list_of_month1[-1]] = str(int(tf1[0])) + "/" + str(int(pg1[0]))
#         df_comparison.at[len(df_comparison) - 1, 'DPT**' + "(" + (list_of_month1[-1]) + ")"] = dpt1
#         # print(df_comparison)
#
#         bar = pd.crosstab(df10["FAULT_OBSERVED"], df10["STAGE"])
#         bar = bar.reset_index()
#         bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})
#         # print(bar)
#         # bar['Faults'] = df_raw['Faults'].str.upper()
#
#         category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
#         category1_df = pd.DataFrame(category)
#
#         category = category.reset_index()
#
#         category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
#         category.loc['Total'] = category.sum(axis=0)
#
#         category = category.tail(1)
#
#         lis_col_names = list(category.columns.values)
#         lis_col_names_all = ["CC ISSUE", "COMPONENT FAIL ISSUE", "COMPONENT DAMAGE/MISS ISSUE",
#                              "MAGNETICS ISSUE",
#                              "DRY SOLDER", "SCRAP", "SOLDERING ISSUE", "WRONG MOUNTING", "REVERSE POLARITY",
#                              "PENDING",
#                              "UNDER ANALYSIS", "RETEST"]
#         for i in lis_col_names_all:
#             if i not in lis_col_names:
#                 category[i] = 0
#         for i in lis_col_names_all:
#             if i not in lis_col_names:
#                 category1_df[i] = 0
#         # category.drop(["PENDING", "UNDER ANALYSIS"], axis=1, inplace=True)
#         comp_quant = category["CC ISSUE"] + category["COMPONENT FAIL ISSUE"]
#         other_quant = category["MAGNETICS ISSUE"] + category["SCRAP"]
#         assmbly_quant = category["COMPONENT DAMAGE/MISS ISSUE"] + category["DRY SOLDER"] + category[
#             "WRONG MOUNTING"] + \
#                         category["REVERSE POLARITY"] + category["SOLDERING ISSUE"]
#         retest_quant = category["RETEST"]
#
#
#
#         # print(m)
#         df_summary_pilot["PRODUCT"] = [m]
#         df_summary_pilot["Qty2"] = pg1
#         df_summary_pilot["Fail2"] = fail_quant1
#         df_summary_pilot["DPT2"] = dpt1
#         df_summary_pilot["Assy."] = assmbly_quant.unique()
#         df_summary_pilot["Comp."] = comp_quant.unique()
#         df_summary_pilot["Retest"] = retest_quant.unique()
#         df_summary_pilot["Others"] = other_quant.unique()
#         df_summary_pilot["Remarks"] = " "
#         # print(df_summary_pilot)
#         df_summ_merged_pilot = pd.concat([df_summ_merged_pilot, df_summary_pilot], axis=0)
#
#         df = df.sort_values(by=['STAGE'], ascending=False)
#
#         partcode_list[m] = str_of_partcode
#         list_of_month_all_pilot[m] = list_of_month1
# if not df_summ_merged.empty:
#     df_summ_merged = df_summ_merged.sort_values(by="DPT2", ascending=False)
#     list_of_products = df_summ_merged["PRODUCT"].to_list()
#
#     df_summ_merged.at[len(df_summ_merged), 'PRODUCT'] = "Total"
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Qty1'] = df_summ_merged["Qty1"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Qty2'] = df_summ_merged["Qty2"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Fail1'] = df_summ_merged["Fail1"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Fail2'] = df_summ_merged["Fail2"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Assy.'] = df_summ_merged["Assy."].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Comp.'] = df_summ_merged["Comp."].sum()
#
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Retest'] = df_summ_merged["Retest"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'Others'] = df_summ_merged["Others"].sum()
#     df_summ_merged.at[len(df_summ_merged) - 1, 'DPT1'] = round(
#         ((df_summ_merged.at[len(df_summ_merged) - 1, 'Fail1'] / df_summ_merged.at[
#             len(df_summ_merged) - 1, 'Qty1']) * 1000),
#         2)
#     # df_summ_merged.at[len(df_summ_merged), 'Remarks'] = ""
#     df_summ_merged.at[len(df_summ_merged) - 1, 'DPT2'] = round(
#         ((df_summ_merged.at[len(df_summ_merged) - 1, 'Fail2'] / df_summ_merged.at[
#             len(df_summ_merged) - 1, 'Qty2']) * 1000),
#         2)
#     df_summ_merged = df_summ_merged.astype(
#         {'Qty1': int, 'Qty2': int, 'Fail1': int, 'Fail2': int, 'Assy.': int, "Retest": int, 'Comp.': int,
#          'Others': int, 'DPT1': int,
#          'DPT2': int})
#     # df_summ_merged.rename(
#     #     columns={'Qty1': 'Test', 'Qty2': "Test", 'Fail1': 'Fail', 'Fail2': 'Fail', 'DPT1': 'DPT**', 'DPT2': 'DPT**',
#     #              'PRODUCT': 'Product'}, inplace=True)
#
# # print(df_summ_merged.columns)
# print(m, df_summ_merged)
# # print(m, df_summ_merged_pilot)
# def update_dataframe_from_doc(df_summ_merged):
#     df_summ_highlight = df_summ_merged.copy()
#     card_nf_cnts = []
#
#     def highlight_row(row):
#         remarks_list = []
#         if row["DPT2"] - row["DPT1"] > 0:
#             remarks_list.append("Improvement Observed")
#         elif row["DPT2"] - row["DPT1"] < 0:
#             remarks_list.append("Increase in DPT")
#
#         if row['Assy.'] == 0:
#             percentage_assy = 0
#         elif row['Fail2'] == 0:
#             percentage_assy = 0
#         else:
#             percentage_assy = round((row['Assy.'] / row['Fail2']) * 100, 0)
#
#         if row['Comp.'] == 0:
#             percentage_comp = 0
#         elif row['Fail2'] == 0:
#             percentage_comp = 0
#         else:
#             percentage_comp = round((row['Comp.'] / row['Fail2']) * 100, 0)
#
#         if percentage_assy > 50:
#             remarks_list.append(str(int(percentage_assy)) + "% faults are due to Assembly issue")
#         elif percentage_comp > 50:
#             remarks_list.append(str(int(percentage_comp)) + "% faults are due to Comp. issue")
#
#         for n in list_of_products:
#             if n in df_pending_value.keys():
#                 if n == row["PRODUCT"]:
#                     remarks_list.append("Pending(" + str(df_pending_value[n]) + ")")
#                 sum_pending_count = df_pending_value[n].sum()
#                 # total_pending += sum_pending_count
#             if n in df_scrap_value.keys():
#                 if n == row["PRODUCT"]:
#                     remarks_list.append("SCRAP(" + str(df_scrap_value[n]) + ")")
#             if n in df_new_fault_text.keys():
#                 if n == row["PRODUCT"]:
#                     if df_new_fault_text[n] is not None:
#                         remarks_list.append("New Fault-(" + df_new_fault_text[n].lower() + ":" +
#                                             str(df_new_fault_value[n]) + ")")
#                         card_nf_cnts.append(n + "(" + str(int(df_new_fault_value[n])) + ")")
#
#         # Join all the remarks in the list with commas and assign to the "Remarks" column
#         row['Remarks'] = ', '.join(remarks_list)
#
#         return row
#
#     df_summ_merged_updated = df_summ_highlight.apply(highlight_row, axis=1)
#
#     return df_summ_merged_updated
#
# df_summ_merged_updated = update_dataframe_from_doc(df_summ_merged)
# dash_columns_pilot = ['Fail2', 'DPT2', 'Assy.', 'Comp.', 'Others', 'Retest', 'Qty2']
# dash_columns = ['Fail1', 'DPT1', 'Qty1']
# df_summ_merged_updated = update_dataframe_from_doc(df_summ_merged)
# for index, row in df_summ_merged_updated.iterrows():
#     if row['Qty1'] == 0:  # pilot
#         for column in dash_columns:
#             df_summ_merged_updated.at[index, column] = '-'
#     if row['Qty2'] == 0:
#         for column in dash_columns_pilot:
#             df_summ_merged_updated.at[index, column] = '-'
# if len(df_card["MONTH"].unique())>2:
#     rename_fx = ['Product', 'Test Qty-('+ df_card["MONTH"].unique()[0]+"-"+df_card["MONTH"].unique()[-2]+ ")", 'Fail Qty-('+ df_card["MONTH"].unique()[0]+"-"+df_card["MONTH"].unique()[-2]+ ")", 'DPT-('+ df_card["MONTH"].unique()[0]+"-"+df_card["MONTH"].unique()[-2]+ ")",'Test Qty-('+ df_card["MONTH"].unique()[-1] +')', 'Fail Qty-('+ df_card["MONTH"].unique()[-1] +')', 'DPT-('+ df_card["MONTH"].unique()[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
#     df_summ_merged_updated.columns = rename_fx
# if len(df_card["MONTH"].unique())==2:
#     rename_fx = ['Product', 'Test Qty-('+ df_card["MONTH"].unique()[0]+")", 'Fail Qty-('+ df_card["MONTH"].unique()[0]+")", 'DPT-('+ df_card["MONTH"].unique()[0]+")",'Test Qty-('+ df_card["MONTH"].unique()[-1] +')', 'Fail Qty-('+ df_card["MONTH"].unique()[-1] +')', 'DPT-('+ df_card["MONTH"].unique()[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
#             df_summ_merged_updated.columns = rename_fx