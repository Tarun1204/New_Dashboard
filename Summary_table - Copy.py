# Import required libraries
import pandas as pd
import numpy as np
import warnings
import re

warnings.simplefilter(action='ignore', category=UserWarning)
pd.options.mode.chained_assignment = None


def table_summary(df):
    try:
        df_table = df.copy()
        df_table.loc[df_table["STAGE"].str.contains('Test', case=False), 'STAGE'] = 'Testing'
        df_table.loc[
            df_table['FAULT_CATEGORY'].str.contains('Fail|Faulty|Faulity', case=False), 'FAULT_CATEGORY'] = 'Comp. Fail'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('broken|damage|dmg|miss|brake',
                                                             case=False), 'FAULT_CATEGORY'] = 'Comp. Miss/Damage'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('Shorting|Solder|short|dry',
                                                             case=False), 'FAULT_CATEGORY'] = 'SOLDER SHORT'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('Magnet|transformer|tfr',
                                                             case=False), 'FAULT_CATEGORY'] = 'Magnetics Issue'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('Polarity', case=False), 'FAULT_CATEGORY'] = 'Polarity'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('cc |Control', case=False), 'FAULT_CATEGORY'] = 'CC Issue'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('ecn', case=False), 'FAULT_CATEGORY'] = 'ECN'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('scrap', case=False), 'FAULT_CATEGORY'] = 'SCRAP'
        df_table.loc[df_table['FAULT_CATEGORY'].str.contains('pending', case=False), 'FAULT_CATEGORY'] = 'PENDING'

        df = df_table.groupby(["FAULT_OBSERVED"], as_index=False).count()

        # for making df with component values
        # a = lambda x: str(x.count()) + '/n ' + '(' + ', '.join(str(v) + ' : ' + str(x.value_counts()[v]) for v in
        #                                                        np.unique(x)) + ')'  # if v.count(",") < 9
        df_data = df_table.pivot_table(columns='FAULT_CATEGORY', index='FAULT_OBSERVED', values='KEY_COMPONENT',
                                       aggfunc=lambda x: str(x.count()) + '/n ' + '(' + ', '.join(str(v)+': ' +
                                                                                                  str(x.value_counts()[v])
                                                                                                  for v in np.unique(x)
                                                                                                  if v.count(",") < 9)+')')\
            .fillna('').reset_index()
        df_data = df_data.rename(columns={"COMPONENT DAMAGE": "Dmg", "COMPONENT MISSING": "Miss",
                                          "FAULT_OBSERVED": "Faults", "Component faulty": "faulty",
                                          "REVERSE POLARITY": "Polarity", "SOLDER SHORT": "Solder"})
        df_data["Faults"] = df_data["Faults"].str.capitalize()

        bar = pd.crosstab(df_table["FAULT_OBSERVED"], df_table["STAGE"])
        bar = bar.reset_index()
        bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})

        category = pd.crosstab(df_table["FAULT_OBSERVED"], df_table["FAULT_CATEGORY"])
        category = category.reset_index()
        category = category.rename(columns={"FAULT_OBSERVED": "Faults"})

        df = df.sort_values(by=['STAGE'], ascending=False)
        total_sum = df["STAGE"].sum()
        # print(total_sum)

        final_df = pd.DataFrame()
        final_df["Faults"] = df["FAULT_OBSERVED"]
        final_df.reset_index()

        for i in range(len(final_df)):
            final_df.at[i, "Total"] = str(df.at[i, "STAGE"]) + "/" + str(total_sum)

            final_df.at[i, "%age"] = str(round(float(df.at[i, "STAGE"] / total_sum) * 100, 1)) + "%"

        final_df = pd.merge(final_df, bar, how="left", on="Faults")
        final_df = pd.merge(final_df, category, how="left", on="Faults")
        # final_df = pd.merge(final_df, df_data, how="left", on="Faults")
        final_df = final_df.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                            "COMPONENT MISSING": "Miss",
                                            "Component faulty": "faulty",
                                            "REVERSE POLARITY": "Polarity",
                                            "SOLDER SHORT": "Solder"})
        final_df["Faults"] = final_df["Faults"].str.capitalize()  # printing data in normal case

    except:
        final_df = pd.DataFrame()

    return final_df

    # final_df
    # final_df.to_excel("Final.xlsx")
    # final_df.to_html("Final.html", index=False, justify="center")
    # print(final_df)
    # final_df


def table_summary_highlights(df_raw_dt, df_card_dt):
    try:
        df_raw1 = df_card_dt
        df_raw_all = df_raw_dt
        month=df_raw1["MONTH"].unique()
        df_raw = df_raw_all[df_raw_all["MONTH"] == month[-1]]
        print(df_raw["PRODUCT_NAME"].unique())
        card=df_card_dt
        raw=df_raw_dt

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
        list_of_month_fst_page = df_raw1['MONTH'].unique()

        def alphaNumOrder(string):
            return ''.join([format(int(x), '05d') if x.isdigit()
                            else x for x in re.split(r'(\d+)', string)])

        df_raw["KEY_COMPONENT"] = df_raw['KEY_COMPONENT'].str.upper().str.replace('&', ',').str.replace(' &',
                                                                                                        ',').str.replace \
            ('& ', ',').str.replace(', ', ',').str.replace(' , ', ',').str.replace(' ,', ',').str.replace('and',
                                                                                                          ',').str.replace(
            ' ', '_')
        df_raw['KEY_COMPONENT'] = df_raw['KEY_COMPONENT'].str.strip(',').str.strip('_')

        # Assuming df_raw is a pandas DataFrame containing a column 'm' with product names
        # condition = df_raw['PRODUCT_NAME'].isin(["TOP(3.3KW_SPIN)", "BOTTOM(3.3KW_SPIN)", "OCPP", "MOV(3.3KW_PIN)","TOP(3.3KW_SPIN_RCMU)", "BOTTOM(3.3KW_SPIN_RCMU)",
        #                                          "MBO_CHARGER", "MAIN CARD(AC001)", "LED CARD(AC001)", "LED CARD(MG)",
        #                                          "LED CARD(TML)", "LCD CARD(AC001)", "LCD/LED CARD", "EMC FILTER", "EVM 100",
        #                                          "CELL CHARGER", "BIC", "BBC CARD", "AC EV_171", "AC EV_211",
        #                                          "AC EV_291", "AC EV(LCD/LED)"])
        # df_raw = df_raw[~condition]

        for m in df_raw["PRODUCT_NAME"].unique():
            # print(m)
            df_raw['FAULT_OBSERVED'] = df_raw['FAULT_OBSERVED'].str.upper()
            df10 = df_raw[df_raw["PRODUCT_NAME"] == m]
            df_pending_date = df10[['FAULT_OBSERVED', 'DATE', 'FAULT_CATEGORY']]
            df_pending_date = df_pending_date[df_pending_date["FAULT_CATEGORY"] == "PENDING"]
            df = df10.groupby(["FAULT_OBSERVED"], as_index=False).count()
            df11 = df10.pivot_table(columns='FAULT_CATEGORY', index="FAULT_OBSERVED", values='KEY_COMPONENT',
                                    aggfunc=lambda x: str(x.count()) + '\n ' + "(" + ', '.join(
                                        str(v) + ': ' + str(x.value_counts()[v]) if (
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
                    str(v) + ': ' + str(x.value_counts()[v]) for v in x.unique() if x.value_counts()[v] < 30)).fillna(
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
            df_remarks = df10.pivot_table(index=["FAULT_OBSERVED", "FAULT_CATEGORY"], values='KEY_COMPONENT',
                                          aggfunc=lambda x: ", ".join(str(v) for v in x.unique())).fillna(
                '').reset_index()

            # print(df_remarks)
            df_remarks = df_remarks.replace({"COMPONENT DAMAGE": "Dmg",
                                             "COMPONENT MISSING": "Miss",
                                             "Component faulty": "faulty",
                                             "REVERSE POLARITY": "Polarity",
                                             "SOLDERING ISSUE": "Solder Short", "DRY SOLDER": "Dry Solder",
                                             "SCRAP": "Scrap",
                                             "MAGNETICS ISSUE": "Magnetics",
                                             "COMPONENT FAIL ISSUE": "Comp. Fail",
                                             "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss", "PENDING": "Pending",
                                             "WRONG MOUNTING": "Wrong Mount", "CC ISSUE": "CC Issue"})

            # print(df_remarks)

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

            df15 = (df_raw1[df_raw1["PRODUCT"] == m]).reset_index()
            # print(df15)
            list_of_partcode = df15.PART_CODE.unique()
            str_of_partcode = ', '.join(list_of_partcode)
            list_of_month1 = df15.MONTH.unique()

            # print(list_of_month1)

            if len(list_of_month1) >= 2:
                df15 = df15[df15["MONTH"] == list_of_month1[-1]]
                df15.at[len(df15) + 10000, "Month"] = list_of_month1[-1]
                df15 = df15.copy()
                # print(df15)

                df15 = df15[df15["MONTH"] == list_of_month1[-1]]
                df15.at[len(df15) + 100000, "Month"] = list_of_month1[-1]
                df15 = df15.copy()

                index = df15[df15['Month'] == list_of_month1[-1]].index.tolist()[0]
                df15.at[index, "Test Quantity"] = df15['TEST_QUANTITY'].sum()
                df15.at[index, "Pass Quantity"] = df15['PASS_QUANTITY'].sum()
                df15.at[index, "Fail Quantity"] = df15['REJECT_QUANTITY'].sum()
                df_crnt_total = df15.tail(1)
                # print(df_crnt_total)
                # print(df_crnt_total["Test Quantity"])

                # print(df_crnt_total.columns)
                df_crnt_total = df_crnt_total.dropna(axis=1)
                # print(df_crnt_total.columns)
                df_crnt_total["DPT**"] = round(((df15['Fail Quantity'] / df15['Test Quantity']) * 1000), 0)
                # print(df_crnt_total)
                pg1 = df_crnt_total["Test Quantity"].unique()
                tf1 = df_crnt_total["Fail Quantity"].unique()
                fail_quant1 = df_crnt_total["Fail Quantity"].unique()
                dpt1 = df_crnt_total["DPT**"].unique()
                df16 = df_raw1[df_raw1["PRODUCT"] == m].reset_index()
                list_of_month2 = df16.MONTH.unique()
                # print(list)
                df16 = df16[df16["MONTH"] != list_of_month2[-1]]
                # df16.reset_index()
                df16.at[len(df16), "Month"] = list_of_month2[0] + "-" + list_of_month2[-2]
                df16.at[len(df16) - 1, "Test Quantity"] = df16['TEST_QUANTITY'].sum()
                df16.at[len(df16) - 1, "Pass Quantity"] = df16['PASS_QUANTITY'].sum()
                df16.at[len(df16) - 1, "Fail Quantity"] = df16['REJECT_QUANTITY'].sum()
                df_previous_month = df16.tail(1)
                df_previous_month = df_previous_month.dropna(axis=1)
                df_previous_month["DPT**"] = round(((df16['Fail Quantity'] / df16['Test Quantity']) * 1000), 0)
                pg2 = df_previous_month["Test Quantity"].unique()
                tf2 = df_previous_month["Fail Quantity"].unique()
                fail_quant2 = df_previous_month["Fail Quantity"].unique()
                dpt2 = df_previous_month["DPT**"].unique()
                # print(dpt2)
                df_new = pd.concat([df_previous_month, df_crnt_total], axis=0)
                # print(df_new)

                # print(df_faults)

                bar = pd.crosstab(df10["FAULT_OBSERVED"], df10["STAGE"])
                bar = bar.reset_index()
                bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})
                # print(bar)
                # bar['Faults'] = df_raw['Faults'].str.upper()

                category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
                category1_df = pd.DataFrame(category)

                category = category.reset_index()

                category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
                category.loc['Total'] = category.sum(axis=0)

                category = category.tail(1)
                # print(category)

                lis_col_names = list(category.columns.values)
                lis_col_names_all = ["CC ISSUE", "COMP. FAIL", "COMP. DMG/MISS", "MAGNETICS ISSUE",
                                     "DRY SOLDER", "SCRAP", "SOLDER SHORT", "WRONG MOUNTING", "REVERSE POLARITY",
                                     "PENDING",
                                     "UNDER ANALYSIS", "RETEST"]
                for i in lis_col_names_all:
                    if i not in lis_col_names:
                        category[i] = 0
                for i in lis_col_names_all:
                    if i not in lis_col_names:
                        category1_df[i] = 0
                # category.drop(["PENDING", "UNDER ANALYSIS"], axis=1, inplace=True)
                comp_quant = category["CC ISSUE"] + category["COMP. FAIL"]
                other_quant = category["MAGNETICS ISSUE"] + category["SCRAP"]
                assmbly_quant = category["COMP. DMG/MISS"] + category["DRY SOLDER"] + category["WRONG MOUNTING"] + \
                                category["REVERSE POLARITY"] + category["SOLDER SHORT"]
                # print(assmbly_quant)
                retest_quant = category["RETEST"]
                category = category.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                                    "COMPONENT MISSING": "Miss",
                                                    "Component faulty": "faulty", "PENDING": "Pending",
                                                    "CC ISSUE": "CC Issue",
                                                    "FAULT_OBSERVED": "Faults",
                                                    "REVERSE POLARITY": "Polarity",
                                                    "SOLDERING ISSUE": "Solder Short",
                                                    "COMPONENT FAIL ISSUE": "Comp. Fail",
                                                    "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss",
                                                    "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
                                                    "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest",
                                                    "SCRAP": "Scrap"})
                df_summary["PRODUCT"] = [m]
                df_summary["Qty1"] = pg2
                df_summary["Fail1"] = fail_quant2
                df_summary["DPT1"] = dpt2
                df_summary["Qty2"] = pg1
                df_summary["Fail2"] = fail_quant1
                df_summary["DPT2"] = dpt1
                df_summary["Assy."] = assmbly_quant.unique()
                df_summary["Comp."] = comp_quant.unique()
                df_summary["Retest"] = retest_quant.unique()
                df_summary["Others"] = other_quant.unique()
                df_summary["Remarks"] = ""
                # df_summary["Remarks"] = ""
                df_summ_merged = pd.concat([df_summ_merged, df_summary], axis=0)

                df = df.sort_values(by=['STAGE'], ascending=False)
                total_sum = df["STAGE"].sum()

                final_df = pd.DataFrame()
                final_df["Faults"] = df["FAULT_OBSERVED"]
                final_df.reset_index()

                for i in range(len(final_df)):
                    final_df.at[i, "Total"] = str(df.at[i, "STAGE"]) + "/" + str(total_sum)

                    final_df.at[i, "%age"] = str(round(float(df.at[i, "STAGE"] / total_sum) * 100, 1)) + "%"

                final_df1 = pd.merge(final_df, bar, how="left", on="Faults")
                final_df1["Faults"] = final_df1["Faults"].str.upper()
                # final_df = pd.merge(final_df,category, how="left", on = "Faults")
                final_df2 = pd.merge(final_df1, df11, how="left", on="Faults")
                # final_df2 = final_df2["Faults"]
                final_df2 = final_df2.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                                      "COMPONENT MISSING": "Miss",
                                                      "Component faulty": "faulty",
                                                      "CC ISSUE": "CC Issue",
                                                      "FAULT_OBSERVED": "Faults",
                                                      "REVERSE POLARITY": "Polarity",
                                                      "SOLDERING ISSUE": "Solder Short",
                                                      "COMPONENT FAIL ISSUE": "Comp. Fail",
                                                      "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss",
                                                      "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
                                                      "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest"})

                df_new = df_new.replace(np.nan, 0)
                final_df2 = final_df2.replace(np.nan, 0)

                # Convert all columns to integer data type
                df_new = df_new.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)
                final_df2 = final_df2.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)
                # df_comparison = df_comparison.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)

                # final_df2.drop(["Testing"], axis=1, inplace=True)
                # df_comparison["REMARKS"] = " "
                final_df2["Remarks"] = " "
                # total line of summary
                final_df2.at[len(final_df2), 'Faults'] = "Total"
                final_df2.at[len(final_df2) - 1, 'Total'] = str(total_sum) + "/" + str(total_sum)
                final_df2.at[len(final_df2) - 1, '%age'] = str(100) + "%"
                # print(category)
                # print(final_df2)
                for column in ["CC Issue", "Polarity", "Solder Short", "Comp. Fail", "Comp. Dmg/Miss", "Magnetics",
                               "Dry Solder", "Wrong Mount", "Retest", "LEAD CUT ISSUE", "PROGRAMMING ISSUE", "Scrap",
                               "Pending",
                               "Under Analysis"]:
                    if column in final_df2.columns:
                        # print(final_df2.columns)
                        final_df2.at[len(final_df2) - 1, column] = category[column].sum()

                final_df2 = final_df2.replace("nan,", "")
                final_df2 = final_df2.replace("nan", "")

                partcode_list[m] = str_of_partcode

                # TO GET PENDING QUANTITY

                def modify_remarks(row):
                    if row["FAULT_CATEGORY"] == "Pending":
                        df_date = df_pending_date[df_pending_date["FAULT_OBSERVED"] == row["FAULT_OBSERVED"]]
                        return "Pending" + "(" + str(row["PENDING"]) + ")" + " since " + str(df_date["DATE"].max())
                    if row["FAULT_CATEGORY"] == "RETEST":
                        return "Retest" + "(" + str(row["RETEST"]) + ")"
                    else:
                        return row["FAULT_CATEGORY"] + "(" + row["KEY_COMPONENT"] + ")"

                # merged_df["REMARKS"] = merged_df.apply(modify_remarks, axis=1)

                # df_remarks = merged_df.groupby("FAULT_OBSERVED").agg(REMARKS=('REMARKS', ', '.join)).reset_index()
                # print(df_remarks)

                # making dictionary of all products
                df_final_total[m] = df_new
                # df_final_comp[m] = df_comparison
                df_final_summary[m] = final_df2
                df_final_remarks[m] = df_remarks
                list_of_month_all[m] = list_of_month2
                category_df[m] = category
            elif len(list_of_month1) == 1:
                # print(list_of_month1)
                df15 = df15[df15["MONTH"] == list_of_month1[-1]]
                df15.at[len(df15) + 3, "Month"] = list_of_month1[-1]
                df15 = df15.copy()
                index = df15[df15['Month'] == list_of_month1[-1]].index.tolist()[0]
                df15.at[index, "Test Quantity"] = df15['TEST_QUANTITY'].sum()
                df15.at[index, "Pass Quantity"] = df15['PASS_QUANTITY'].sum()
                df15.at[index, "Fail Quantity"] = df15['REJECT_QUANTITY'].sum()
                df_crnt_total = df15.tail(1)
                # [print(df_crnt_total)]
                df_crnt_total = df_crnt_total.dropna(axis=1)
                df_crnt_total["DPT**"] = round(((df15['Fail Quantity'] / df15['Test Quantity']) * 1000), 0)
                pg1 = df_crnt_total["Test Quantity"].unique()
                tf1 = df_crnt_total["Fail Quantity"].unique()
                fail_quant1 = df_crnt_total["Fail Quantity"].unique()
                dpt1 = df_crnt_total["DPT**"].unique()
                df_new = df_crnt_total.copy()
                df18 = df_raw[df_raw["PRODUCT_NAME"] == m].reset_index()
                df_faults2 = df18[['FAULT_OBSERVED', "DATE"]]
                table2 = df_faults2.pivot_table(index='FAULT_OBSERVED', aggfunc='count')
                table2 = table2.reset_index()
                table2.rename(columns={'DATE': list_of_month1[-1]}, inplace=True)
                table2['DPT**' + "(" + (list_of_month1[-1]) + ")"] = round((table2[list_of_month1[-1]] / pg1) * 1000, 0)
                df_comparison = table2
                df_comparison.at[len(df_comparison), 'FAULT_OBSERVED'] = "Grand Total"
                df_comparison.at[len(df_comparison) - 1, list_of_month1[-1]] = str(int(tf1[0])) + "/" + str(int(pg1[0]))
                df_comparison.at[len(df_comparison) - 1, 'DPT**' + "(" + (list_of_month1[-1]) + ")"] = dpt1
                # print(df_comparison)

                bar = pd.crosstab(df10["FAULT_OBSERVED"], df10["STAGE"])
                bar = bar.reset_index()
                bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})
                # print(bar)
                # bar['Faults'] = df_raw['Faults'].str.upper()

                category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
                category1_df = pd.DataFrame(category)

                category = category.reset_index()

                category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
                category.loc['Total'] = category.sum(axis=0)

                category = category.tail(1)

                lis_col_names = list(category.columns.values)
                lis_col_names_all = ["CC ISSUE", "COMPONENT FAIL ISSUE", "COMPONENT DAMAGE/MISS ISSUE",
                                     "MAGNETICS ISSUE",
                                     "DRY SOLDER", "SCRAP", "SOLDERING ISSUE", "WRONG MOUNTING", "REVERSE POLARITY",
                                     "PENDING",
                                     "UNDER ANALYSIS", "RETEST"]
                for i in lis_col_names_all:
                    if i not in lis_col_names:
                        category[i] = 0
                for i in lis_col_names_all:
                    if i not in lis_col_names:
                        category1_df[i] = 0
                # category.drop(["PENDING", "UNDER ANALYSIS"], axis=1, inplace=True)
                comp_quant = category["CC ISSUE"] + category["COMPONENT FAIL ISSUE"]
                other_quant = category["MAGNETICS ISSUE"] + category["SCRAP"]
                assmbly_quant = category["COMPONENT DAMAGE/MISS ISSUE"] + category["DRY SOLDER"] + category[
                    "WRONG MOUNTING"] + \
                                category["REVERSE POLARITY"] + category["SOLDERING ISSUE"]
                retest_quant = category["RETEST"]
                df_summary_pilot["PRODUCT"] = [m]
                df_summary_pilot["Qty2"] = pg1
                df_summary_pilot["Fail2"] = fail_quant1
                df_summary_pilot["DPT2"] = dpt1
                df_summary_pilot["Assy."] = assmbly_quant.unique()
                df_summary_pilot["Comp."] = comp_quant.unique()
                df_summary_pilot["Retest"] = retest_quant.unique()
                df_summary_pilot["Others"] = other_quant.unique()
                df_summary_pilot["Remarks"] = " "
                # print(df_summary_pilot)
                df_summ_merged_pilot = pd.concat([df_summ_merged_pilot, df_summary_pilot], axis=0)

                df = df.sort_values(by=['STAGE'], ascending=False)
                total_sum = df["STAGE"].sum()

                final_df = pd.DataFrame()
                final_df["Faults"] = df["FAULT_OBSERVED"]
                final_df.reset_index()

                for i in range(len(final_df)):
                    final_df.at[i, "Total"] = str(df.at[i, "STAGE"]) + "/" + str(total_sum)

                    final_df.at[i, "%age"] = str(round(float(df.at[i, "STAGE"] / total_sum) * 100, 1)) + "%"

                final_df1 = pd.merge(final_df, bar, how="left", on="Faults")
                final_df1["Faults"] = final_df1["Faults"].str.upper()
                # final_df = pd.merge(final_df,category, how="left", on = "Faults")
                final_df2 = pd.merge(final_df1, df11, how="left", on="Faults")
                # final_df2 = final_df2["Faults"]
                final_df2 = final_df2.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                                      "COMPONENT MISSING": "Miss",
                                                      "Component faulty": "faulty",
                                                      "CC ISSUE": "CC Issue",
                                                      "FAULT_OBSERVED": "Faults",
                                                      "REVERSE POLARITY": "Polarity",
                                                      "SOLDERING ISSUE": "Solder Short",
                                                      "COMPONENT FAIL ISSUE": "Comp. Fail",
                                                      "COMPONENT DAMAGE/MISS ISSUE": "Comp. Dmg/Miss",
                                                      "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
                                                      "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest"})

                df_new = df_new.replace(np.nan, 0)
                final_df2 = final_df2.replace(np.nan, 0)

                df_comparison = df_comparison.replace(np.nan, 0)
                # Convert all columns to integer data type
                df_new = df_new.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)
                final_df2 = final_df2.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)
                df_comparison = df_comparison.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)

                # final_df2.drop(["Testing"], axis=1, inplace=True)
                df_comparison["REMARKS"] = " "
                final_df2["Remarks"] = " "

                partcode_list[m] = str_of_partcode

                # TO GET PENDING QUANTITY
                for i in range(len(df_remarks)):
                    cell_value = df_remarks.iloc[i]['KEY_COMPONENT']
                    cell_value = cell_value.split(",")
                    key_list = [s.strip(',') for s in cell_value]
                    key_list.sort(key=alphaNumOrder)
                    df_remarks.at[i, 'KEY_COMPONENT'] = ",".join(list(set(key_list)))

                    df_remarks["REMARKS"] = df_remarks.apply(
                        lambda row: "{}({})".format(row["FAULT_CATEGORY"], str(row["KEY_COMPONENT"])), axis=1)
                    # print(key_list)

                category1_df.reset_index()
                merged_df = pd.merge(df_remarks, category1_df["PENDING"], on="FAULT_OBSERVED")
                merged_df = pd.merge(merged_df, category1_df["RETEST"], on="FAULT_OBSERVED")
                df_pending_date['DATE'] = pd.to_datetime(df_pending_date['DATE']).dt.date

                def modify_remarks(row):
                    # print(row)
                    if row["FAULT_CATEGORY"] == "Pending":
                        df_date = df_pending_date[df_pending_date["FAULT_OBSERVED"] == row["FAULT_OBSERVED"]]
                        return "Pending" + "(" + str(row["PENDING"]) + ")" + " since " + str(df_date["DATE"].max())
                    if row["FAULT_CATEGORY"] == "RETEST":
                        return "Retest" + "(" + str(row["RETEST"]) + ")"

                    else:
                        return row["FAULT_CATEGORY"] + "(" + row["KEY_COMPONENT"] + ")"

                merged_df["REMARKS"] = merged_df.apply(modify_remarks, axis=1)

                # Group by FAULT_OBSERVED and aggregate the REMARKS column
                df_remarks = merged_df.groupby("FAULT_OBSERVED").agg(REMARKS=('REMARKS', ', '.join)).reset_index()
                # print(df_remarks)

                # making dictionary of all products
                df_final_total_pilot[m] = df_new
                df_final_comp_pilot[m] = df_comparison
                df_final_summary_pilot[m] = final_df2
                df_final_remarks_pilot[m] = df_remarks
                list_of_month_all_pilot[m] = list_of_month1
                category_df[m] = category

        if not df_summ_merged.empty:

            df_summ_merged = df_summ_merged.sort_values(by="DPT2", ascending=False)
            list_of_products = df_summ_merged["PRODUCT"].to_list()

            df_summ_merged.at[len(df_summ_merged), 'PRODUCT'] = "Total"
            df_summ_merged.at[len(df_summ_merged) - 1, 'Qty1'] = df_summ_merged["Qty1"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Qty2'] = df_summ_merged["Qty2"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Fail1'] = df_summ_merged["Fail1"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Fail2'] = df_summ_merged["Fail2"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Assy.'] = df_summ_merged["Assy."].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Comp.'] = df_summ_merged["Comp."].sum()

            df_summ_merged.at[len(df_summ_merged) - 1, 'Retest'] = df_summ_merged["Retest"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'Others'] = df_summ_merged["Others"].sum()
            df_summ_merged.at[len(df_summ_merged) - 1, 'DPT1'] = round(
                ((df_summ_merged.at[len(df_summ_merged) - 1, 'Fail1'] / df_summ_merged.at[
                    len(df_summ_merged) - 1, 'Qty1']) * 1000),
                2)
            # df_summ_merged.at[len(df_summ_merged), 'Remarks'] = ""
            df_summ_merged.at[len(df_summ_merged) - 1, 'DPT2'] = round(
                ((df_summ_merged.at[len(df_summ_merged) - 1, 'Fail2'] / df_summ_merged.at[
                    len(df_summ_merged) - 1, 'Qty2']) * 1000),
                2)
            df_summ_merged = df_summ_merged.astype(
                {'Qty1': int, 'Qty2': int, 'Fail1': int, 'Fail2': int, 'Assy.': int, "Retest": int, 'Comp.': int,
                 'Others': int, 'DPT1': int,
                 'DPT2': int})
            # df_summ_merged.rename(
            #     columns={'Qty1': 'Test', 'Qty2': "Test", 'Fail1': 'Fail', 'Fail2': 'Fail', 'DPT1': 'DPT**', 'DPT2': 'DPT**',
            #              'PRODUCT': 'Product'}, inplace=True)

        # print(df_summ_merged.columns)


        df_summ_merged=df_summ_merged[df_summ_merged["PRODUCT"]!="Total"]

        second_df=df_summ_merged.copy()
        run = -1
        for prod in card['PRODUCT'].unique():

            # print(prod)
            df_prod = card[card['PRODUCT'] == prod]
            prod_months = df_prod['MONTH'].unique()
            month = card["MONTH"].unique()
            ##################################################

            if month[-1] not in prod_months:
                run += 1
                # print(run)
                qty1 = df_prod['TEST_QUANTITY'].sum()
                fail1 = df_prod['REJECT_QUANTITY'].sum()
                dpt1 = int(round(fail1 / qty1 * 1000, 0))
                qty2, fail2, dpt2, ass, comp, ret, oth = 0, 0, 0, 0, 0, 0, 0

                new_row = {'PRODUCT': prod, 'Qty1': qty1, 'Fail1': fail1, 'DPT1': dpt1, 'Qty2': qty2, 'Fail2': fail2,
                           'DPT2': dpt2, 'Assy.': ass,
                           'Comp.': comp, 'Retest': ret, 'Others': oth, 'Remarks': ''}
                second_df = pd.concat([second_df, pd.DataFrame([new_row])], ignore_index=True)

            if len(prod_months) == 1:
                p = prod_months[0]
                if p == month[-1]:  # PILOT
                    run += 1
                    # print(run)
                    qty2 = df_prod['TEST_QUANTITY'].sum()
                    fail2 = df_prod['REJECT_QUANTITY'].sum()
                    dpt2 = int(round(fail2 / qty2 * 1000, 0))
                    qty1, fail1, dpt1 = 0, 0, 0

                    # raw.to_excel('a.xlsx')
                    assembly_df = raw[raw['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS",
                                                                  "DRY SOLDER", "WRONG MOUNTING", "REVERSE POLARITY"
                                                                     , "LEAD CUT ISSUE",
                                                                  "OPERATOR FAULT", "COATING ISSUE"])]
                    comp_df = raw[raw['FAULT_CATEGORY'].isin(["COMP. FAIL", "CC ISSUE"])]
                    retest_df = raw[raw['FAULT_CATEGORY'] == 'RETEST']
                    other_df = raw[raw['FAULT_CATEGORY'].isin(["MAGNETICS ISSUE", "SCRAP"])]

                    assembly_df = assembly_df[assembly_df['PRODUCT_NAME'] == prod]
                    comp_df = comp_df[comp_df['PRODUCT_NAME'] == prod]
                    retest_df = retest_df[retest_df['PRODUCT_NAME'] == prod]
                    other_df = other_df[other_df['PRODUCT_NAME'] == prod]

                    ass_q = len(assembly_df)
                    comp_q = len(comp_df)
                    ret_q = len(retest_df)
                    oth_q = len(other_df)

                    new_row = {'PRODUCT': prod, 'Qty1': qty1, 'Fail1': fail1, 'DPT1': dpt1, 'Qty2': qty2,
                               'Fail2': fail2,
                               'DPT2': dpt2, 'Assy.': ass_q,
                               'Comp.': comp_q, 'Retest': ret_q, 'Others': oth_q, 'Remarks': 'Pilot Lot'}
                    second_df = pd.concat([second_df, pd.DataFrame([new_row])], ignore_index=True)

        dash_columns_pilot = ['Fail2', 'DPT2', 'Assy.', 'Comp.', 'Others', 'Retest', 'Qty2']
        dash_columns = ['Fail1', 'DPT1', 'Qty1']

        for index, row in second_df.iterrows():
            if row['Qty1'] == 0:  # pilot
                for column in dash_columns:
                    second_df.at[index, column] = 0
            if row['Qty2'] == 0:
                for column in dash_columns_pilot:
                    second_df.at[index, column] = 0

        sum_qty1 = second_df['Qty1'].sum()
        sum_fail1 = second_df['Fail1'].sum()
        sum_dpt1 = int(round(sum_fail1 / sum_qty1 * 1000))
        sum_qty2 = second_df['Qty2'].sum()
        sum_fail2 = second_df['Fail2'].sum()
        sum_dpt2 = int(round(sum_fail2 / sum_qty2 * 1000))
        sum_ass = second_df['Assy.'].sum()
        sum_com = second_df['Comp.'].sum()
        sum_ret = second_df['Retest'].sum()
        sum_oth = second_df['Others'].sum()

        new_row = {'PRODUCT': 'Total', 'Qty1': sum_qty1, 'Fail1': sum_fail1, 'DPT1': sum_dpt1, 'Qty2': sum_qty2,
                   'Fail2': sum_fail2,
                   'DPT2': sum_dpt2, 'Assy.': sum_ass,
                   'Comp.': sum_com, 'Retest': sum_ret, 'Others': sum_oth, 'Remarks': "-"}
        second_df = pd.concat([second_df, pd.DataFrame([new_row])], ignore_index=True)

        second_df.to_excel('aa.xlsx')

        def update_dataframe_from_doc(second_df):
            df_summ_highlight = second_df.copy()
            card_nf_cnts = []
            percentage_assy = 0
            percentage_comp = 0

            def highlight_row(row):
                remarks_list = []

                if row['DPT1'] == '-':
                    remarks_list.append('Pilot Lot')
                if row['DPT2'] == '-':
                    remarks_list.append('-')
                elif row['DPT2'] != '-' and row['DPT1'] != '-':
                    if row["DPT2"] - row["DPT1"] > 0:
                        remarks_list.append("Increase in DPT")
                    else:
                        pass
                elif row['DPT2'] != "-" and row['DPT1'] != '-':
                    if row["DPT2"] - row["DPT1"] < 0:
                        remarks_list.append("Improvement Observed")

                if row['Assy.'] == 0:
                    percentage_assy = 0
                elif row['Fail2'] == 0:
                    percentage_assy = 0
                elif row['Assy.'] == '-':
                    percentage_assy = 0
                elif row['Fail2'] == '-':
                    percentage_assy = 0
                else:
                    percentage_assy = round((row['Assy.'] / row['Fail2']) * 100, 0)

                if row['Qty1'] == 0:
                    remarks_list.append("Pilot Lot Run.")

                if 'Pilot Lot Run.' in remarks_list:
                    remarks_list = ['Pilot Lot Run']

                if row['Fail2'] == 0:
                    row['Assy.'] = '-'
                    row['Comp.'] = '-'
                    row['Retest'] = '-'
                    row['Others'] = '-'

                if row['Comp.'] == 0:
                    percentage_comp = 0
                elif row['Fail2'] == 0:
                    percentage_comp = 0
                elif row['Comp.'] == '-':

                    percentage_comp = 0
                elif row['Fail2'] == '-':
                    percentage_comp = 0
                else:
                    percentage_comp = round((row['Comp.'] / row['Fail2']) * 100, 0)
                    # print(percentage_comp)

                if percentage_assy > 50:
                    remarks_list.append(str(int(percentage_assy)) + "% faults are due to Assembly issue")
                elif percentage_comp > 50:
                    remarks_list.append(str(int(percentage_comp)) + "% faults are due to Comp. issue")

                for n in list_of_products:
                    if n in df_pending_value.keys():
                        if n == row["PRODUCT"]:
                            remarks_list.append("Pending(" + str(df_pending_value[n]) + ")")
                        sum_pending_count = df_pending_value[n].sum()
                        # total_pending += sum_pending_count
                    if n in df_scrap_value.keys():
                        if n == row["PRODUCT"]:
                            remarks_list.append("SCRAP(" + str(df_scrap_value[n]) + ")")
                    if n in df_new_fault_text.keys():
                        if n == row["PRODUCT"]:
                            if df_new_fault_text[n] is not None:
                                remarks_list.append("New Fault-(" + df_new_fault_text[n].lower() + ":" +
                                                    str(df_new_fault_value[n]) + ")")
                                card_nf_cnts.append(n + "(" + str(int(df_new_fault_value[n])) + ")")

                # Join all the remarks in the list with commas and assign to the "Remarks" column
                row['Remarks'] = ', '.join(remarks_list)

                # print(remarks_list)

                return row

            df_summ_highlight = df_summ_highlight.apply(highlight_row, axis=1)

            return df_summ_highlight

        dash_columns_pilot = ['Fail2', 'DPT2', 'Assy.', 'Comp.', 'Others', 'Retest', 'Qty2']
        dash_columns = ['Fail1', 'DPT1', 'Qty1']
        df_summ_merged_updated = update_dataframe_from_doc(df_summ_merged)
        for index, row in df_summ_merged_updated.iterrows():
            if row['Qty1'] == 0:  # pilot
                for column in dash_columns:
                    df_summ_merged_updated.at[index, column] = '-'
            if row['Qty2'] == 0:
                for column in dash_columns_pilot:
                    df_summ_merged_updated.at[index, column] = '-'
        if len(df_raw1["MONTH"].unique())>2:
            rename_fx = ['Product', 'Test Qty-('+ df_raw1["MONTH"].unique()[0]+"-"+df_raw1["MONTH"].unique()[-2]+ ")", 'Fail Qty-('+ df_raw1["MONTH"].unique()[0]+"-"+df_raw1["MONTH"].unique()[-2]+ ")", 'DPT-('+ df_raw1["MONTH"].unique()[0]+"-"+df_raw1["MONTH"].unique()[-2]+ ")",'Test Qty-('+ df_raw1["MONTH"].unique()[-1] +')', 'Fail Qty-('+ df_raw1["MONTH"].unique()[-1] +')', 'DPT-('+ df_raw1["MONTH"].unique()[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
            df_summ_merged_updated.columns = rename_fx
        if len(df_raw1["MONTH"].unique())==2:
            rename_fx = ['Product', 'Test Qty-('+ df_raw1["MONTH"].unique()[0]+")", 'Fail Qty-('+ df_raw1["MONTH"].unique()[0]+")", 'DPT-('+ df_raw1["MONTH"].unique()[0]+")",'Test Qty-('+ df_raw1["MONTH"].unique()[-1] +')', 'Fail Qty-('+ df_raw1["MONTH"].unique()[-1] +')', 'DPT-('+ df_raw1["MONTH"].unique()[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
            df_summ_merged_updated.columns = rename_fx


    except Exception as e:
        print(f"Error: {e}")
        df_summ_merged_updated = pd.DataFrame()

    return df_summ_merged_updated


def table_summary_select_all(df_raw_all, df_card_all):
    try:
        final_df = 0
        df_card = df_card_all
        df_raw = df_raw_all

        row_values = ['F1-DCT (Old Designs)', 'F1-DCT (New Designs)', 'F1-EVSE', 'F2 (Old Designs)', 'F2 (New Designs)']

        def month_data(df_card_data):
            months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
            df_month = df_card_data['MONTH'].unique()
            # print(df_month)
            sorted(df_month, key=lambda x: months.index(x.split(",")[0]))
            month_order = sorted(df_month, key=lambda x: (int(x.split(",")[1]), months.index(x.split(",")[0])))

            return month_order

        month_order = month_data(df_card)

        # fty_perc_prev_col = f'FTY%-({month_order[0]}-{month_order[-2]})'
        # dpt_prev_col = f'DPT-({month_order[0]}-{month_order[-2]})'
        # fty_perc_curr_col = f'FTY%-({month_order[-1]})'
        # dpt_curr_col = f'DPT-({month_order[-1]})'

        fty_perc_prev_col = f'FTY%-({month_order[0]}-{month_order[0]})'
        dpt_prev_col = f'DPT-({month_order[0]}-{month_order[0]})'
        fty_perc_curr_col = f'FTY%-({month_order[-1]})'
        dpt_curr_col = f'DPT-({month_order[-1]})'

        data = {"Design": row_values, fty_perc_prev_col: 0, dpt_prev_col: 0, fty_perc_curr_col: 0, dpt_curr_col: 0,
                'Major Faults': 0, 'Remarks': 0}
        df = pd.DataFrame(data)

        run = -1
        all_designs = ['DCT_OLD', 'DCT_NEW', 'EVSE', 'F2_NEW', 'F2_OLD']
        # all_designs=['F2_OLD']
        # all_designs=['DCT_OLD','DCT_NEW','EVSE']
        for des in all_designs:
            run += 1
            # print(des)
            df_dct_old = df_card[df_card['DESIGN'] == des]
            # print(df_dct_old)
            last_month_df_dct_old=df_dct_old[df_dct_old['MONTH']==month_order[-1]]
            df_dct_old_prev = df_dct_old.groupby(["MONTH"]).sum(numeric_only=True).reset_index()
            dct_old_prev_fty_rows = df_dct_old_prev[df_dct_old_prev['MONTH']!=month_order[-1]]
            # print(dct_old_prev_fty_rows['MONTH'].unique())

            if (last_month_df_dct_old['REJECT_QUANTITY'].sum()==0):

                dct_old_prev_fty_value = dct_old_prev_fty_rows['PASS_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 100
                dct_old_prev_fty_value = round(dct_old_prev_fty_value, 1)
                dct_old_prev_dpt_value = dct_old_prev_fty_rows['REJECT_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 1000
                dct_old_prev_dpt_value = int(round(dct_old_prev_dpt_value, 0))


                dct_old_curr_fty_value = 0
                dct_old_curr_dpt_value = 0
                dct_old_major_faults = '-'
                final_remarks = '-'

                if 'DCT' in des:
                    if 'OLD' in des:
                        des2 = 'F1-DCT (Old Designs)'
                    else:
                        des2 = 'F1-DCT (New Designs)'
                elif des == 'EVSE':
                    des2 = 'F1-EVSE'
                elif 'F2' in des:
                    if 'OLD' in des:
                        des2 = 'F2 (Old Designs)'
                    else:
                        des2 = 'F2 (New Designs)'

                df.loc[df['Design'] == des2, df.columns[1]] = dct_old_prev_fty_value
                df.loc[df['Design'] == des2, df.columns[2]] = dct_old_prev_dpt_value
                df.loc[df['Design'] == des2, df.columns[3]] = float(dct_old_curr_fty_value)
                df.loc[df['Design'] == des2, df.columns[4]] = dct_old_curr_dpt_value
                df.loc[df['Design'] == des2, df.columns[5]] = dct_old_major_faults
                df.loc[df['Design'] == des2, df.columns[6]] = final_remarks

            elif len(month_order)==1:
                # print('in loop')
                dct_old_prev_fty_value=0
                dct_old_prev_dpt_value=0
                # print(last_month_df_dct_old)
                dct_old_curr_fty_value = (last_month_df_dct_old['PASS_QUANTITY'].sum() / last_month_df_dct_old['TEST_QUANTITY'].sum()) * 100
                dct_old_curr_fty_value = round(dct_old_curr_fty_value, 1)
                dct_old_curr_dpt_value = (last_month_df_dct_old['REJECT_QUANTITY'].sum() / last_month_df_dct_old[
                    'TEST_QUANTITY'].sum()) * 1000
                # print(dct_old_curr_dpt_value,type(dct_old_curr_dpt_value))
                dct_old_curr_dpt_value = (round(dct_old_curr_dpt_value, 0))

                df_dct_old_raw2=df_raw[df_raw['MONTH']==month_order[0]]
                df_dct_old_raw2 = df_raw[df_raw['DESIGN'] == des]
                pending_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'PENDING']
                # print(pending_df)
                pending_qty = len(pending_df)
                if pending_qty == 0:
                    pending_remarks = "No Pending."
                else:
                    pending_remarks = f'Pending-{pending_qty}.'

                scrap_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'SCRAP']
                scrap_qty = len(scrap_df)
                if scrap_qty == 0:
                    scrap_remarks = ''
                else:
                    scrap_products = scrap_df['PRODUCT_NAME'].unique()
                    scrap_products = ','.join(scrap_products)
                    scrap_remarks = f'{scrap_qty} Scraped in {scrap_products}.'

                flag = True
                total_faults = len(df_dct_old_raw2)
                if total_faults==0:
                    flag=False

                if flag:
                    assembly = df_dct_old_raw2[
                        df_dct_old_raw2['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS",
                                                                "WRONG MOUNTING", "REVERSE POLARITY", "SOLDER SHORT"])]
                    assembly_qty = len(assembly)
                    assembly_prcnt = assembly_qty / total_faults * 100
                    assembly_prcnt = int(round(assembly_prcnt, 0))

                    comp = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'].isin(["COMP. FAIL", "CC ISSUE"])]
                    comp_qty = len(comp)
                    comp_prcnt = comp_qty / total_faults * 100
                    comp_prcnt = int(round(comp_prcnt, 0))

                    assembly_group = assembly.groupby(['PRODUCT_NAME']).count().reset_index("PRODUCT_NAME")
                    comp_group = comp.groupby(['PRODUCT_NAME']).count().reset_index("PRODUCT_NAME")

                    assembly_40 = assembly_qty * 0.3
                    comp_40 = comp_qty * 0.3

                    # assembly_group.to_excel('aa.xlsx')
                    assemble_per_product = {}
                    for i in assembly_group['PRODUCT_NAME']:
                        result_value = assembly_group.loc[assembly_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        assemble_per_product[i] = result_value
                    # print(assemble_per_product)
                    # print('assemble_per_product', assemble_per_product)
                    comp_per_product = {}
                    for i in comp_group['PRODUCT_NAME']:
                        result_value2 = comp_group.loc[comp_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        comp_per_product[i] = result_value2

                    assembly_remarks = []
                    for i in assemble_per_product.keys():
                        if assemble_per_product[i] > assembly_40:
                            assembly_remarks.append(f'{i}[{str(assemble_per_product[i])}]')
                            # print(assembly_remarks)
                    # print('assembly_remarks', assembly_remarks)
                    sorted_assembly_remarks = sorted(assembly_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                     reverse=True)

                    assembly_remarks_str = ', '.join(sorted_assembly_remarks)
                    # print('assembly_remarks_str', assembly_remarks_str)
                    comp_remarks = []
                    for i in comp_per_product.keys():
                        if comp_per_product[i] > comp_40:
                            comp_remarks.append(f'{i}[{str(comp_per_product[i])}]')
                    sorted_comp_remarks = sorted(comp_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                 reverse=True)
                    comp_remarks_str = ','.join(sorted_comp_remarks)

                    if comp_prcnt == 0:
                        dct_old_major_faults = f"{assembly_prcnt}% of the faults are due to Assembly issue({assembly_qty})-{assembly_remarks_str}"
                    else:
                        dct_old_major_faults = f"{assembly_prcnt}% of the faults are due to Assembly issue({assembly_qty})-{(assembly_remarks_str)},\n{comp_prcnt}% of the faults are due to Component issue({comp_qty})-{comp_remarks_str}"

                    if scrap_qty == 0:
                        final_remarks = f'{pending_remarks}'
                    else:
                        final_remarks = f'{scrap_remarks}\n{pending_remarks}'


                if 'DCT' in des:
                    if 'OLD' in des:
                        des2 = 'F1-DCT (Old Designs)'
                    else:
                        des2 = 'F1-DCT (New Designs)'
                elif des == 'EVSE':
                    des2 = 'F1-EVSE'
                elif 'F2' in des:
                    if 'OLD' in des:
                        des2 = 'F2 (Old Designs)'
                    else:
                        des2 = 'F2 (New Designs)'

                df.loc[df['Design'] == des2, df.columns[1]] = dct_old_prev_fty_value
                df.loc[df['Design'] == des2, df.columns[2]] = dct_old_prev_dpt_value
                df.loc[df['Design'] == des2, df.columns[3]] = (dct_old_curr_fty_value)
                df.loc[df['Design'] == des2, df.columns[4]] = dct_old_curr_dpt_value
                df.loc[df['Design'] == des2, df.columns[5]] = dct_old_major_faults
                df.loc[df['Design'] == des2, df.columns[6]] = final_remarks


            else:
                # df_dct_old = df_dct_old.fillna(0)
                df_for_zero = df_dct_old
                # df_dct_old.to_excel('oo.xlsx')

                df_dct_old['MONTH'] = pd.Categorical(df_dct_old['MONTH'], categories=month_order, ordered=True)

                # DCT OLD
                # print(df_dct_old)
                df_dct_old_prev = df_dct_old.groupby(["MONTH"]).sum(numeric_only=True).reset_index()
                # print(df_dct_old_prev)
                df_dct_old_curr = df_dct_old.groupby(["MONTH", 'PRODUCT']).sum(numeric_only=True).reset_index()
                # print(df_dct_old_curr['PRODUCT'].unique())
                partcodes = df_dct_old_curr['PRODUCT'].unique()
                # print(partcodes)

                ########$%^&*(IUFDHJKUY#$%^&*UGCVG
                rows_with_0 = df_dct_old_curr[df_dct_old_curr['S_No'] == 0].index
                df_dct_old_curr.drop(rows_with_0, inplace=True)
                df_dct_old_curr.reset_index(drop=True, inplace=True)
                # (df_dct_old_curr.to_excel('pqrs.xlsx'))

                for i in partcodes:
                    count = 0
                    for j in df_dct_old_curr['PRODUCT']:
                        if i == j:
                            count += 1
                        if count >= 2:
                            break
                    # print(i,count)
                    if (count <= 1) and (df_dct_old_curr['MONTH'] == month_order[-1]).any():
                        # print("inside loop")
                        rows_to_remove = df_dct_old_curr[df_dct_old_curr['PRODUCT'] == i]
                        df_dct_old_curr = df_dct_old_curr.drop(rows_to_remove.index)
                # print(df_dct_old_curr)
                # print(df_dct_old_curr['PRODUCT'].unique())

                # print(df_dct_old_prev)
                dct_old_prev_fty_rows = df_dct_old_prev.iloc[:-1]
                dct_old_prev_fty_value = dct_old_prev_fty_rows['PASS_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 100
                dct_old_prev_fty_value = round(dct_old_prev_fty_value, 1)
                dct_old_prev_dpt_value = dct_old_prev_fty_rows['REJECT_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 1000
                dct_old_prev_dpt_value = int(round(dct_old_prev_dpt_value, 0))

                # print('o test q= ', int(dct_old_prev_fty_rows['TEST_QUANTITY'].sum()))
                # print('o pass q= ', dct_old_prev_fty_rows['PASS_QUANTITY'].sum())
                # print('o fail q= ', dct_old_prev_fty_rows['REJECT_QUANTITY'].sum())

                dct_old_curr_fty_row = df_dct_old_prev.tail(1)
                # print(dct_old_curr_fty_row)
                # print(dct_old_curr_fty_row)
                # current_month=dct_old_curr_fty_row['MONTH']
                # current_month=current_month[1:9]
                # current_month=current_month.upper()
                # current_month=current_month.to_string(index=False)
                # print(current_month)
                dct_old_curr_fty_value = dct_old_curr_fty_row['PASS_QUANTITY'] / dct_old_curr_fty_row['TEST_QUANTITY'] * 100
                # print('n test q= ',dct_old_curr_fty_row['TEST_QUANTITY'] )
                # print('n pass q= ', dct_old_curr_fty_row['PASS_QUANTITY'])
                # print('n fail q= ', dct_old_curr_fty_row['REJECT_QUANTITY'])
                dct_old_curr_fty_value = round(dct_old_curr_fty_value, 1)
                dct_old_curr_dpt_value = dct_old_curr_fty_row['REJECT_QUANTITY'] / dct_old_curr_fty_row[
                    'TEST_QUANTITY'] * 1000
                # print(dct_old_curr_dpt_value)
                dct_old_curr_dpt_value = int(round(dct_old_curr_dpt_value, 0))

                # print("dct_old_curr_fty_value=",dct_old_curr_fty_value)
                # print("dct_old_prev_fty_value=",dct_old_prev_fty_value)
                # print("dct_old_curr_dpt_value=",dct_old_curr_dpt_value)
                # print("dct_old_prev_dpt_value=",dct_old_prev_dpt_value)

                # Categories

                fc = ['DRY SOLDER', 'COMP. FAIL', 'COMP. DMG/MISS', 'SOLDER SHORT', 'SCRAP', 'MAGNETICS ISSUE',
                      'UNDER ANALYSIS', 'CC ISSUE',
                      'WRONG MOUNTING', 'RETEST', 'REVERSE POLARITY', 'COATING ISSUE', 'Other', 'OPERATOR FAULT',
                      'NOT FOUND', 'CALIBRATION ISSUE', 'PROG. ISSUE', 'LEAD CUT ISSUE', 'ECN NOT DONE']
                fault_cat = {}
                for i in fc:
                    fault_cat[i] = 0

                # DCT OLD REMARKS
                ################################################################################################################3#####

                df_dct_old_raw = df_raw[(df_raw['DESIGN'] == des)]
                df_dct_old_raw['MONTH'] = pd.Categorical(df_dct_old_raw['MONTH'], categories=month_order, ordered=True)
                products_to_keep = df_dct_old_curr['PRODUCT'].unique()
                # print(products_to_keep)
                df_dct_old_raw = df_dct_old_raw[
                    df_dct_old_raw['PRODUCT_NAME'].isin(products_to_keep)]  # all months without pilot
                df_dct_old_wo_latest_month = df_dct_old_raw[
                    df_dct_old_raw['MONTH'] != month_order[-1]]  # all months without pilot without last
                # print('JUN,23' in df_dct_old_wo_latest_month['MONTH'].unique())

                # print(df_dct_old_raw)
                df_dct_old_raw2 = df_dct_old_raw[df_dct_old_raw['MONTH'] == month_order[-1]]  # latest month without pilot
                # print(df_dct_old_raw2)
                # print(df_dct_old_raw['PRODUCT_NAME'].unique())

                # Pending
                pending_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'PENDING']
                # print(pending_df)
                pending_qty = len(pending_df)
                if pending_qty == 0:
                    pending_remarks = "No Pending."
                else:
                    pending_remarks = f'Pending-{pending_qty}.'
                ##
                # Retest
                retest_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'RETEST']
                retest_qty = len(retest_df)
                #
                # SCRAP
                scrap_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'SCRAP']
                scrap_qty = len(scrap_df)
                if scrap_qty == 0:
                    scrap_remarks = ''
                else:
                    scrap_products = scrap_df['PRODUCT_NAME'].unique()
                    scrap_products = ','.join(scrap_products)
                    scrap_remarks = f'{scrap_qty} Scraped in {scrap_products}.'
                #
                # new_fault
                new_fault = []

                # df_dct_old_raw2.to_excel('1.xlsx')
                # df_dct_old_wo_latest_month.to_excel('2.xlsx')
                faults_per_product = {}
                for product in df_dct_old_raw2['PRODUCT_NAME'].unique():
                    fault = 0
                    product_df = df_dct_old_raw2[df_dct_old_raw2['PRODUCT_NAME'] == product]
                    # product_df.to_excel('3.xlsx')
                    prev_product_df = df_dct_old_wo_latest_month[df_dct_old_wo_latest_month['PRODUCT_NAME'] == product]
                    # prev_product_df.to_excel('4.xlsx')
                    for fo in product_df['FAULT_OBSERVED'].unique():
                        if fo in prev_product_df['FAULT_OBSERVED'].unique():
                            pass
                        else:
                            fault += 1
                            new_fault.append((product, fault, fo))
                # new_fault_final={}
                # for x in new_fault:
                #     new_fault_final[x[0]]=[]
                # new_fault_1=new_fault
                # new_fault_2=new_fault
                # for m in range(len(new_fault_1)-1):
                #     new_fault_final[new_fault[m][0]].append(new_fault[m][1])
                #     for n in range(len(new_fault_2)-1):
                #         if new_fault_1[m][1]!=new_fault_2[n][1] and new_fault_1[m][0]==new_fault_2[n][0]:
                #             new_fault_final[new_fault[m][0]].append(new_fault[m][1])
                #             del new_fault_2[n]
                # print(new_fault_final)

                # formatted_list = [f"{item[0]}({item[1]}) - {item[2]}" for item in new_fault]
                # print(new_fault)
                blank_dict = {}
                for i in new_fault:
                    # print("Iam I,",i)
                    if i[0] in blank_dict.keys():
                        blank_dict[i[0]] = blank_dict[i[0]] + [(i[2])]
                    else:
                        blank_dict[i[0]] = [i[2]]
                # print(blank_dict)

                formatted_list = [f"{item}({len(blank_dict[item])}) - {blank_dict[item]}" for item in blank_dict.keys()]
                modified_list = []
                for item in formatted_list:
                    prefix, failures = item.split(" - ")
                    failures = failures.strip("[]").replace("'", "")
                    modified_list.append(f"{prefix} - {failures}")

                new_fault_str = ", ".join(modified_list)

                # new_fault_str = ", ".join(formatted_list)

                if new_fault == [] and scrap_qty == 0:
                    final_remarks = f'{pending_remarks}'
                elif new_fault == []:
                    final_remarks = f'{scrap_remarks}\n{pending_remarks}'
                elif scrap_qty==0 and new_fault!=[]:
                    final_remarks = f'New Fault: {new_fault_str}.\n{pending_remarks}'
                else:
                    final_remarks = f'New Fault: {new_fault_str}.\n{scrap_remarks}\n{pending_remarks}'

                # print(df_dct_old_raw2['PRODUCT_NAME'].unique())
                # print(df_dct_old_raw2)
                total_faults_dct_old = len(df_dct_old_raw2)
                # print(total_faults_dct_old)
                flag = True
                # print(des)
                if total_faults_dct_old == 0:
                    # print(total_faults_dct_old)
                    flag = False  # 0 faults of latest month

                assembly = df_dct_old_raw2[
                    df_dct_old_raw2['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS",
                                                            "WRONG MOUNTING", "REVERSE POLARITY", "SOLDER SHORT"])]
                # print(assembly['PRODUCT_NAME'].unique())
                # print(des)
                if flag:

                    # print('Flag', flag)
                    assembly_qty = len(assembly)
                    assembly_prcnt = assembly_qty / total_faults_dct_old * 100
                    assembly_prcnt = int(round(assembly_prcnt, 0))
                    # print(assembly_prcnt,'assembly_prcnt')

                    comp = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'].isin(["COMP. FAIL", "CC ISSUE"])]
                    comp_qty = len(comp)
                    comp_prcnt = comp_qty / total_faults_dct_old * 100
                    comp_prcnt = int(round(comp_prcnt, 0))
                    # print('comp_prcnt',comp_prcnt)

                    assembly_group = assembly.groupby(['PRODUCT_NAME']).count().reset_index("PRODUCT_NAME")
                    comp_group = comp.groupby(['PRODUCT_NAME']).count().reset_index("PRODUCT_NAME")

                    assembly_40 = assembly_qty * 0.3
                    comp_40 = comp_qty * 0.3

                    # assembly_group.to_excel('aa.xlsx')
                    assemble_per_product = {}
                    for i in assembly_group['PRODUCT_NAME']:
                        result_value = assembly_group.loc[assembly_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        assemble_per_product[i] = result_value
                    # print(assemble_per_product)
                    # print('assemble_per_product',assemble_per_product)
                    comp_per_product = {}
                    for i in comp_group['PRODUCT_NAME']:
                        result_value2 = comp_group.loc[comp_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        comp_per_product[i] = result_value2

                    assembly_remarks = []
                    for i in assemble_per_product.keys():
                        if assemble_per_product[i] > assembly_40:
                            assembly_remarks.append(f'{i}[{str(assemble_per_product[i])}]')
                            # print(assembly_remarks)
                    # print('assembly_remarks',assembly_remarks)
                    sorted_assembly_remarks = sorted(assembly_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                     reverse=True)

                    assembly_remarks_str = ', '.join(sorted_assembly_remarks)
                    # print('assembly_remarks_str',assembly_remarks_str)
                    comp_remarks = []
                    for i in comp_per_product.keys():
                        if comp_per_product[i] > comp_40:
                            comp_remarks.append(f'{i}[{str(comp_per_product[i])}]')
                    sorted_comp_remarks = sorted(comp_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                     reverse=True)
                    comp_remarks_str = ','.join(sorted_comp_remarks)

                    if comp_prcnt == 0:
                        dct_old_major_faults = f"{assembly_prcnt}% of the faults are due to Assembly issue({assembly_qty})-{assembly_remarks_str}"
                    else:
                        dct_old_major_faults = f"{assembly_prcnt}% of the faults are due to Assembly issue({assembly_qty})-{(assembly_remarks_str)},\n{comp_prcnt}% of the faults are due to Component issue({comp_qty})-{comp_remarks_str}"
                    dct_old_row_values = [dct_old_prev_fty_value, dct_old_prev_dpt_value, dct_old_curr_fty_value,
                                          dct_old_curr_dpt_value, dct_old_major_faults]

                # else:
                #     print('Flag',flag)
                #     dct_old_curr_fty_value = 0
                #     dct_old_curr_dpt_value = 0
                #     dct_old_major_faults = '-'
                #     final_remarks = '-'

                if 'DCT' in des:
                    if 'OLD' in des:
                        des2 = 'F1-DCT (Old Designs)'
                    else:
                        des2 = 'F1-DCT (New Designs)'
                elif des == 'EVSE':
                    des2 = 'F1-EVSE'
                elif 'F2' in des:
                    if 'OLD' in des:
                        des2 = 'F2 (Old Designs)'
                    else:
                        des2 = 'F2 (New Designs)'
                # print(dct_old_curr_fty_value)
                df.loc[df['Design'] == des2, df.columns[1]] = dct_old_prev_fty_value
                df.loc[df['Design'] == des2, df.columns[2]] = dct_old_prev_dpt_value
                df.loc[df['Design'] == des2, df.columns[3]] = float(dct_old_curr_fty_value)
                df.loc[df['Design'] == des2, df.columns[4]] = dct_old_curr_dpt_value
                df.loc[df['Design'] == des2, df.columns[5]] = dct_old_major_faults
                df.loc[df['Design'] == des2, df.columns[6]] = final_remarks

                # df.columns=new_header
                # print(df)


        if len(month_order)==1:
            df= df.drop(df.columns[1], axis=1)
            df = df.drop(df.columns[1], axis=1)
        elif len(month_order)==2:
            # print('2list',month_order)
            rename_fx = ['Design', f'FTY%-({month_order[0]})', f'DPT-({month_order[0]})', f'FTY%-({month_order[-1]})',
                         f'DPT-({month_order[-1]})', 'Major Faults', 'Remarks']

            # Rename the columns using the rename_fx list
            df.columns = rename_fx
        elif len(month_order)> 2:
            # print('3 list',month_order)
            rename_fx = ['Design', f'FTY%-({month_order[0]}-{month_order[-2]})', f'DPT-({month_order[0]}-{month_order[-2]})', f'FTY%-({month_order[-1]})',
                         f'DPT-({month_order[-1]})', 'Major Faults', 'Remarks']

            # Rename the columns using the rename_fx list
            df.columns = rename_fx
        final_df = df
        # printing data in normal case
    except Exception as e:
        print(f"Error: {e}")
        final_df = pd.DataFrame()
    # except :
    #     final_df = pd.DataFrame()

    return final_df





def table_summary_product(df_raw_all, df_card_all):
    try:
        df_card = df_card_all
        df_raw = df_raw_all

        # initialize dictionaries
        df_final_remarks, partcode_list, list_of_month_all = {}, {}, {}
        df_retest_value, df_pending_value, df_scrap_value = {}, {}, {}


        df_raw["KEY_COMPONENT"] = df_raw['KEY_COMPONENT'].str.upper().str.replace('&', ',').str.replace(' &',
                                                                                                        ',').str.replace \
            ('& ', ',').str.replace(', ', ',').str.replace(' , ', ',').str.replace(' ,', ',').str.replace('and',
                                                                                                          ',').str.replace(
            ' ', '_')
        df_raw['KEY_COMPONENT'] = df_raw['KEY_COMPONENT'].str.strip(',').str.strip('_')

        import re
        def alphaNumOrder(string):
            return ''.join([format(int(x), '05d') if x.isdigit()
                            else x for x in re.split(r'(\d+)', string)])

        # Assuming df_raw is a pandas DataFrame containing a column 'm' with product names
        # condition = df_raw['PRODUCT_NAME'].isin(["TOP(3.3KW_SPIN)", "BOTTOM(3.3KW_SPIN)", "OCPP", "MOV(3.3KW_PIN)","TOP(3.3KW_SPIN_RCMU)", "BOTTOM(3.3KW_SPIN_RCMU)",
        #                                          "MBO_CHARGER", "MAIN CARD(AC001)", "LED CARD(AC001)", "LED CARD(MG)",
        #                                          "LED CARD(TML)", "LCD CARD(AC001)", "LCD/LED CARD", "EMC FILTER", "EVM 100",
        #                                          "CELL CHARGER", "BIC", "BBC CARD", "AC EV_171", "AC EV_211",
        #                                          "AC EV_291", "AC EV(LCD/LED)"])
        # df_raw = df_raw[~condition]

        for m in df_raw["PRODUCT_NAME"].unique():
            # print(m)
            df_raw['FAULT_OBSERVED'] = df_raw['FAULT_OBSERVED'].str.upper()
            df10 = df_raw[df_raw["PRODUCT_NAME"] == m]
            df_pending_date = df10[['FAULT_OBSERVED', 'DATE', 'FAULT_CATEGORY']]
            df_pending_date = df_pending_date[df_pending_date["FAULT_CATEGORY"] == "PENDING"]
            df = df10.groupby(["FAULT_OBSERVED"], as_index=False).count()
            df11 = df10.pivot_table(columns='FAULT_CATEGORY', index="FAULT_OBSERVED", values='KEY_COMPONENT',
                                    aggfunc=lambda x: str(x.count()) + '\n ' + "(" + ', '.join(
                                        str(v) + ': ' + str(x.value_counts()[v]) if (
                                                    str(v) != "NOT_FOUND" and str(v) != "NFF") else "" for v in
                                        x.unique() if x.value_counts()[v] < 50) + ")").fillna('').reset_index()

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
                df_retest_count["RETEST"].replace([np.inf, -np.inf], np.nan,
                                                  inplace=True)  # Handle infinite values by replacing with NaN
                df_retest_count["RETEST"] = pd.to_numeric(df_retest_count["RETEST"], errors='coerce').astype(
                    float)  # Convert to float instead of int
                df_retest_count["RETEST"].fillna(0, inplace=True)  # Replace any remaining NaN with 0
                df_retest_value[m] = df_retest_count["RETEST"].sum()

            df_only_key_component = df10.pivot_table(columns='FAULT_CATEGORY', index="FAULT_OBSERVED",
                                                     values='KEY_COMPONENT', aggfunc=lambda x: ', '.join(
                    str(v) + ': ' + str(x.value_counts()[v]) for v in x.unique() if x.value_counts()[v] < 30)).fillna(
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
            df_remarks = df10.pivot_table(index=["FAULT_OBSERVED", "FAULT_CATEGORY"], values='KEY_COMPONENT',
                                          aggfunc=lambda x: ", ".join(str(v) for v in x.unique())).fillna(
                '').reset_index()

            # print(df_remarks)
            # print(df_remarks)

            df11 = df11.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                        "COMPONENT MISSING": "Miss",
                                        "Component faulty": "faulty",
                                        "FAULT_OBSERVED": "Faults",
                                        "REVERSE POLARITY": "Polarity",
                                        "SOLDER SHORT": "Solder Short", "DRY SOLDER": "Dry Solder", "SCRAP": "Scrap",
                                        "MAGNETICS ISSUE": "Magnetics",
                                        "COMP. FAIL": "Comp. Fail",
                                        "COMP. DMG/MISS": "Comp. Dmg/Miss", "PENDING": "Pending",
                                        "WRONG MOUNTING": "Wrong Mount", "CC ISSUE": "CC Issue", "RETEST": "Retest"})
            df11["Faults"] = df11["Faults"].str.upper()

            df15 = (df_raw[df_raw["PRODUCT_NAME"] == m]).reset_index()
            # print(df15)
            list_of_partcode = df15.PART_CODE.unique()
            str_of_partcode = ', '.join(list_of_partcode)
            list_of_month1 = df15.MONTH.unique()

            # print(list_of_month1)

            bar = pd.crosstab(df10["FAULT_OBSERVED"], df10["STAGE"])
            bar = bar.reset_index()
            bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})
            # print(bar)
            # bar['Faults'] = df_raw['Faults'].str.upper()

            category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
            category1_df = pd.DataFrame(category)

            category = category.reset_index()

            category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
            category.loc['Total'] = category.sum(axis=0)

            category = category.tail(1)
            # print(category)

            lis_col_names = list(category.columns.values)
            lis_col_names_all = ["CC ISSUE", "COMP. FAIL", "COMP. DMG/MISS", "MAGNETICS ISSUE",
                                 "DRY SOLDER", "SCRAP", "SOLDER SHORT", "WRONG MOUNTING", "REVERSE POLARITY",
                                 "PENDING",
                                 "UNDER ANALYSIS", "RETEST"]
            for i in lis_col_names_all:
                if i not in lis_col_names:
                    category[i] = 0
            for i in lis_col_names_all:
                if i not in lis_col_names:
                    category1_df[i] = 0
            category = category.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                                "COMPONENT MISSING": "Miss",
                                                "Component faulty": "faulty", "PENDING": "Pending",
                                                "CC ISSUE": "CC Issue",
                                                "FAULT_OBSERVED": "Faults",
                                                "REVERSE POLARITY": "Polarity",
                                                "SOLDER SHORT": "Solder Short",
                                                "COMP. FAIL": "Comp. Fail",
                                                "COMP. DMG/MISS": "Comp. Dmg/Miss",
                                                "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
                                                "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest", "SCRAP": "Scrap"})

            df = df.sort_values(by=['STAGE'], ascending=False)
            total_sum = df["STAGE"].sum()

            final_df = pd.DataFrame()
            final_df["Faults"] = df["FAULT_OBSERVED"]
            final_df.reset_index()

            for i in range(len(final_df)):
                final_df.at[i, "Total"] = str(df.at[i, "STAGE"]) + "/" + str(total_sum)

                final_df.at[i, "%age"] = str(round(float(df.at[i, "STAGE"] / total_sum) * 100, 1)) + "%"

            final_df1 = pd.merge(final_df, bar, how="left", on="Faults")
            final_df1["Faults"] = final_df1["Faults"].str.upper()
            # final_df = pd.merge(final_df,category, how="left", on = "Faults")
            final_df2 = pd.merge(final_df1, df11, how="left", on="Faults")
            # final_df2 = final_df2["Faults"]
            final_df2 = final_df2.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                                  "COMPONENT MISSING": "Miss",
                                                  "Component faulty": "faulty",
                                                  "CC ISSUE": "CC Issue",
                                                  "FAULT_OBSERVED": "Faults",
                                                  "REVERSE POLARITY": "Polarity",
                                                  "SOLDER SHORT": "Solder Short",
                                                  "COMP. FAIL": "Comp. Fail",
                                                  "COMP. DMG/MISS": "Comp. Dmg/Miss",
                                                  "MAGNETICS ISSUE": "Magnetics", "DRY SOLDER": "Dry Solder",
                                                  "WRONG MOUNTING": "Wrong Mount", "RETEST": "Retest"})

            final_df2 = final_df2.replace(np.nan, 0)

            # Convert all columns to integer data type
            final_df2 = final_df2.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)

            final_df2["Remarks"] = " "
            # total line of summary
            final_df2.at[len(final_df2), 'Faults'] = "Total"
            final_df2.at[len(final_df2) - 1, 'Total'] = str(total_sum) + "/" + str(total_sum)
            final_df2.at[len(final_df2) - 1, '%age'] = str(100) + "%"
            # print(category)
            # print(final_df2)
            for column in ["CC Issue", "Polarity", "Solder Short", "Comp. Fail", "Comp. Dmg/Miss", "Magnetics",
                           "Dry Solder", "Wrong Mount", "Retest", "LEAD CUT ISSUE", "PROGRAMMING ISSUE", "Scrap",
                           "Pending",
                           "Under Analysis"]:
                if column in final_df2.columns:
                    final_df2.at[len(final_df2) - 1, column] = category[column].sum()

            final_df2 = final_df2.replace("nan,", "")
            final_df2 = final_df2.replace("nan", "")
            partcode_list[m] = str_of_partcode
        del final_df2["Remarks"]
        columns_to_remove = [col for col in final_df2.columns if 'Testing' in col]
        columns_to_remove1 = [col for col in final_df2.columns if 'TESTING' in col]

        # Step 2: Remove the identified columns from the DataFrame
        final_df2.drop(columns=columns_to_remove, inplace=True)

        final_df2.drop(columns=columns_to_remove1, inplace=True)
        final_df2=final_df2.replace(np.nan, 0)

    except:
        final_df2 = pd.DataFrame()

    return final_df2