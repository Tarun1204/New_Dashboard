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
        final_df["Faults"] = final_df["Faults"].str.capitalize()

    except:
        final_df = pd.DataFrame()

    return final_df

    # final_df
    # final_df.to_excel("Final.xlsx")
    # final_df.to_html("Final.html", index=False, justify="center")

    # final_df


def table_summary_highlights(df_raw_dt, df_card_dt):
    try:
        rem1 = None
        rem2 = None
        df_card = df_card_dt
        df_raw = df_raw_dt
        month=df_card["MONTH"].unique()

        # card=df_card_dt

        def month_data(df_card_data):
            months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
            df_month = df_card_data['MONTH'].unique()
            sorted(df_month, key=lambda x: months.index(x.split(",")[0]))
            month_order = sorted(df_month, key=lambda x: (int(x.split(",")[1]), months.index(x.split(",")[0])))

            return month_order

        month_order = month_data(df_card)

        df_card["MONTH"] = pd.Categorical(df_card['MONTH'], categories=month_order, ordered=True)

        df_raw["MONTH"] = pd.Categorical(df_raw['MONTH'], categories=month_order, ordered=True)
        df_raw = df_raw[df_raw["MONTH"] == month_order[-1]]

        month=df_card["MONTH"].unique()

        df_raw = df_raw[df_raw["MONTH"] == month_order[-1]]
        df_card_current_month = df_card[df_card["MONTH"] == month_order[-1]]
        df_card_previous_month = df_card[df_card["MONTH"].isin(month_order[0:-1])]


        # initialize dataframes
        df_crnt_total, df_previous_month = pd.DataFrame(), pd.DataFrame()
        df_summ_merged_raw, df_summ_merged_not_in_raw = pd.DataFrame(), pd.DataFrame()

        df_retest_value, df_pending_value, df_scrap_value = {}, {}, {}
        df_new_fault_text, df_new_fault_value = {}, {}

        for m in df_card["PRODUCT"].unique():

            df_current_mnth = df_card[df_card["PRODUCT"] == m].reset_index()

            df15 = df_current_mnth[df_current_mnth["MONTH"] == month_order[-1]]
            df_tail = pd.DataFrame(columns=["Product", "Test(" + month_order[-1] + ")", "Pass(" + month_order[-1] + ")",
                                            "Fail(" + month_order[-1] + ")", "DPT(" + month_order[-1] + ")"])
            # df_tail.loc[0, "Month"] = list_of_month[-1]
            df_tail.loc[0, "Product"] = m
            tq = df15['TEST_QUANTITY'].sum()
            pq = df15['PASS_QUANTITY'].sum()
            rq = df15['REJECT_QUANTITY'].sum()

            df_tail.loc[0, "Test(" + month_order[-1] + ")"] = tq
            df_tail.loc[0, "Pass(" + month_order[-1] + ")"] = pq
            df_tail.loc[0, "Fail(" + month_order[-1] + ")"] = rq

            if tq == 0:
                df_tail.loc[0, "DPT(" + month_order[-1] + ")"] = 0
            else:
                df_tail.loc[0, "DPT(" + month_order[-1] + ")"] = int(round((rq / tq) * 1000, 0))

            df_summ_merged_raw = pd.concat([df_summ_merged_raw, df_tail], axis=0)


        for m in df_card["PRODUCT"].unique():

            df_previous_month = df_card[df_card["PRODUCT"] == m].reset_index()
            df15 = df_previous_month.copy()
            df15 = df15[df15["MONTH"].isin(month_order[0:-1])]
            df_tail_previous = pd.DataFrame(
                columns=["Product", "Test Quantity", "Pass Quantity", "Fail Quantity", "DPT"])

            df_tail_previous.loc[0, "Product"] = m
            tq = df15['TEST_QUANTITY'].sum()
            pq = df15['PASS_QUANTITY'].sum()
            rq = df15['REJECT_QUANTITY'].sum()
            df_tail_previous.loc[0, "Test Quantity"] = tq
            df_tail_previous.loc[0, "Pass Quantity"] = pq
            df_tail_previous.loc[0, "Fail Quantity"] = rq

            df_tail_previous = df_tail_previous.fillna(0)
            if tq == 0:
                df_tail_previous.loc[0, "DPT"] = 0
            else:
                df_tail_previous.loc[0, "DPT"] = int(round((rq / tq) * 1000, 0))


            df_summ_merged_not_in_raw = pd.concat([df_summ_merged_not_in_raw, df_tail_previous], axis=0)


        df_summary_highlights = pd.merge(df_summ_merged_not_in_raw, df_summ_merged_raw, on="Product")

        df_summary_highlights = df_summary_highlights.fillna(0)

        df_summary_highlights = df_summary_highlights.sort_values(by=["DPT(" + month_order[-1] + ")"], ascending=False)
        # Identify rows where "DPT" is not equal to 0
        non_zero_rows = df_summary_highlights["DPT"] != 0
        df_non_zero = df_summary_highlights[non_zero_rows]
        df_zero = df_summary_highlights[~non_zero_rows]
        df_summary_highlights = pd.concat([df_non_zero, df_zero])
        raw=df_raw
        card=df_card
        for prod in df_summary_highlights["Product"]:
            qty2 = df_summary_highlights["Test(" + month_order[-1] + ")"].sum()
            fail2 = df_summary_highlights["Fail(" + month_order[-1] + ")"].sum()
            if qty2 == 0:
                dpt2 = 0
            else:
                dpt2 = int(round(fail2 / qty2 * 1000, 0))
            qty1, fail1, dpt1 = 0, 0, 0

            # raw.to_excel('a.xlsx')
            assembly_df = raw[raw['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS",
                                                          "DRY SOLDER", "WRONG MOUNT", "REVERSE POLARITY",
                                                          "LEAD CUT ISSUE",
                                                          "OPERATOR FAULT", "COATING ISSUE", "WRONG COMPONENT"])]
            # assembly_df.to_excel('ass.xlsx')
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

            df_summary_highlights.loc[
                df_summary_highlights['Product'] == prod, ['Assy.', 'Comp.', 'Retest', 'Others']] = [ass_q, comp_q,
                                                                                                     ret_q, oth_q]

        sum_qty1 = df_summary_highlights['Test Quantity'].sum()
        sum_fail1 = df_summary_highlights['Fail Quantity'].sum()

        sum_pass1 = df_summary_highlights["Pass Quantity"].sum()
        if sum_qty1 == 0:
            sum_dpt1 = 0
        else:
            sum_dpt1 = int(round(sum_fail1 / sum_qty1 * 1000))
        sum_qty2 = df_summary_highlights["Test(" + month_order[-1] + ")"].sum()
        sum_fail2 = df_summary_highlights["Fail(" + month_order[-1] + ")"].sum()
        sum_pass2 = df_summary_highlights["Pass(" + month_order[-1] + ")"].sum()
        if sum_qty2 == 0:
            sum_dpt2 = 0
        else:
            sum_dpt2 = int(round(sum_fail2 / sum_qty2 * 1000))
        sum_ass = df_summary_highlights['Assy.'].sum()
        sum_com = df_summary_highlights['Comp.'].sum()
        sum_ret = df_summary_highlights['Retest'].sum()
        sum_oth = df_summary_highlights['Others'].sum()

        if sum_dpt2 > sum_dpt1:
            rem1 = 'Increase in DPT, '
        if rem1==None:
            rem1=''
        if sum_dpt2 < sum_dpt1:
            rem1 = 'Improvement Observed,'
        if sum_com == 0 or sum_fail2 == 0:
            comp_perc = 0
        else:
            comp_perc = int(round(sum_com / sum_fail2 * 100, 0))
        if sum_ass == 0 or sum_fail2 == 0:
            ass_perc = 0
        else:
            ass_perc = int(round(sum_ass / sum_fail2 * 100, 0))
        rem2 = f'{ass_perc}% faults are due to Assembly issue and {comp_perc}% faults are due to Comp. issue'
        remarks = rem1 + rem2

        new_row = {'Product': 'Total', 'Test Quantity': sum_qty1, 'Pass Quantity': sum_pass1,
                   'Fail Quantity': sum_fail1, 'DPT': sum_dpt1, "Test(" + month_order[-1] + ")": sum_qty2,
                   "Pass(" + month_order[-1] + ")": sum_pass2,
                   "Fail(" + month_order[-1] + ")": sum_fail2,
                   "DPT(" + month_order[-1] + ")": sum_dpt2, 'Assy.': sum_ass,
                   'Comp.': sum_com, 'Retest': sum_ret, 'Others': sum_oth, 'Remarks': remarks}
        new_row_df = pd.DataFrame([new_row])

        # Concatenate the existing DataFrame with the new row
        df_summary_highlights = pd.concat([df_summary_highlights, new_row_df], ignore_index=True)
        # df_summary_highlights = df_summary_highlights.append(new_row, ignore_index=True)
        dash_columns_pilot = ["Fail(" + month_order[-1] + ")", "DPT(" + month_order[-1] + ")",
                              "Pass(" + month_order[-1] + ")", 'Assy.', 'Comp.', 'Others', 'Retest',
                              "Test(" + month_order[-1] + ")"]
        dash_columns = ['Fail Quantity', 'Pass Quantity', 'DPT', 'Test Quantity']

        for index, row in df_summary_highlights.iterrows():
            if row['Test Quantity'] == 0:  # pilot
                for column in dash_columns:
                    df_summary_highlights.at[index, column] = 0000
            if row["Test(" + month_order[-1] + ")"] == 0:
                for column in dash_columns_pilot:
                    df_summary_highlights.at[index, column] = 0000

        list_of_products = df_summary_highlights["Product"].unique()
        # second_df.to_excel('aa.xlsx')
        df_summary_highlights = df_summary_highlights.fillna(0)

        def update_dataframe_from_doc(df_summary_highlights):
            df_summary_highlights = df_summary_highlights.copy()
            card_nf_cnts = []
            percentage_assy = 0
            percentage_comp = 0

            def highlight_row(row):
                remarks_list = []

                if row['DPT'] == 0000 and (row['Test Quantity']==0 and row['Fail Quantity']==0):
                    if len(month_order)>2:
                        remarks_list.append('Pilot Lot')
                    else:
                        pass
                if row["DPT(" + month_order[-1] + ")"] == 0000 and (row['Test(' + month_order[-1] + ")"] == 0):
                    remarks_list.append('-')
                elif row["DPT(" + month_order[-1] + ")"] != 0000 and row['DPT'] != 0000:
                    if row["DPT(" + month_order[-1] + ")"] - row["DPT"] > 0:
                        remarks_list.append("Increase in DPT")
                    else:
                        pass
                elif row["DPT(" + month_order[-1] + ")"] != "-" and row['DPT'] != 0000:
                    if row["DPT(" + month_order[-1] + ")"] - row["DPT"] < 0:
                        remarks_list.append("Improvement Observed")

                if row['Assy.'] == 0:
                    percentage_assy = 0
                elif row["Fail(" + month_order[-1] + ")"] == 0:
                    percentage_assy = 0
                elif row['Assy.'] == 0000:
                    percentage_assy = 0
                elif row["Fail(" + month_order[-1] + ")"] == 0000:
                    percentage_assy = 0
                else:
                    percentage_assy = round((row['Assy.'] / row["Fail(" + month_order[-1] + ")"]) * 100, 0)

                if row["Fail(" + month_order[-1] + ")"] == 0:
                    row['Assy.'] = '-'
                    row['Comp.'] = '-'
                    row['Retest'] = '-'
                    row['Others'] = '-'

                if row['Comp.'] == 0:
                    percentage_comp = 0
                elif row["Fail(" + month_order[-1] + ")"] == 0:
                    percentage_comp = 0
                elif row['Comp.'] == 0000:
                    percentage_comp = 0
                elif row["Fail(" + month_order[-1] + ")"] == 0000:
                    percentage_comp = 0
                else:
                    percentage_comp = round((row['Comp.'] / row["Fail(" + month_order[-1] + ")"]) * 100, 0)


                if percentage_assy > 50:
                    remarks_list.append(str(int(percentage_assy)) + "% faults are due to Assembly issue")
                elif percentage_comp > 50:
                    remarks_list.append(str(int(percentage_comp)) + "% faults are due to Comp. issue")

                for n in list_of_products:
                    if n in df_pending_value.keys():
                        if n == row["Product"]:
                            remarks_list.append("Pending(" + str(df_pending_value[n]) + ")")
                        sum_pending_count = df_pending_value[n].sum()
                        # total_pending += sum_pending_count
                    if n in df_scrap_value.keys():
                        if n == row["Product"]:
                            remarks_list.append("SCRAP(" + str(df_scrap_value[n]) + ")")
                    if n in df_new_fault_text.keys():
                        if n == row["Product"]:
                            if df_new_fault_text[n] is not None:
                                remarks_list.append("New Fault-(" + df_new_fault_text[n].lower() + ":" +
                                                    str(df_new_fault_value[n]) + ")")
                                card_nf_cnts.append(n + "(" + str(int(df_new_fault_value[n])) + ")")

                # Join all the remarks in the list with commas and assign to the "Remarks" column
                row['Remarks'] = ', '.join(remarks_list)



                return row

            df_summ = df_summary_highlights.apply(highlight_row, axis=1)

            return df_summ

        df_summ_merged_updated = update_dataframe_from_doc(df_summary_highlights)
        # df_summ_merged_updated.to_excel("df_final.xlsx")
        #
        if len(month_order)>2:
            rename_fx = ['Product', 'Test Qty-('+ month_order[0]+"-"+month_order[-2]+ ")", "Pass Qty-(" + month_order[0]+"-"+month_order[-2]+ ")", 'Fail Qty-('+ month_order[0]+"-"+month_order[-2]+ ")", 'DPT-('+ month_order[0]+"-"+month_order[-2]+ ")",'Test Qty-('+ month_order[-1] +')', "Pass Qty-(" + month_order[-1]+ ")", 'Fail Qty-('+ month_order[-1] +')', 'DPT-('+ month_order[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
            df_summ_merged_updated.columns = rename_fx
        if len(month_order)==2:
            rename_fx = ['Product', 'Test Qty-('+ month_order[0]+")", "Pass Qty-(" + month_order[0] + ")", 'Fail Qty-('+ month_order[0]+")", 'DPT-('+ month_order[0]+")",'Test Qty-('+ month_order[-1] +')', "Pass Qty-(" + month_order[-1] + ")", 'Fail Qty-('+ month_order[-1] +')', 'DPT-('+ month_order[-1] +')', 'Assy.', 'Comp.', 'Retest', 'Others','Remarks']
            df_summ_merged_updated.columns = rename_fx
        if len(month_order) == 1:
            df_summ_merged_updated = df_summ_merged_updated.drop(df_summ_merged_updated.columns[1:5], axis=1)
            # df_summ_merged_updated = df_summ_merged_updated.drop(df_summ_merged_updated.columns[1], axis=1)
            # df_summ_merged_updated = df_summ_merged_updated.drop(df_summ_merged_updated.columns[1], axis=1)
            # df_summ_merged_updated = df_summ_merged_updated.drop(df_summ_merged_updated.columns[1], axis=1)


        # "Fail(" + month_order[-1] + ")", "DPT(" + month_order[-1] + ")",
        # "Pass(" + month_order[-1] + ")", 'Assy.', 'Comp.', 'Others', 'Retest',
        # "Test(" + month_order[-1] + ")"


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

            df_dct_old = df_card[df_card['DESIGN'] == des]

            last_month_df_dct_old=df_dct_old[df_dct_old['MONTH']==month_order[-1]]
            df_dct_old_prev = df_dct_old.groupby(["MONTH"]).sum(numeric_only=True).reset_index()
            dct_old_prev_fty_rows = df_dct_old_prev[df_dct_old_prev['MONTH']!=month_order[-1]]


            if (last_month_df_dct_old['REJECT_QUANTITY'].sum()==0):

                dct_old_prev_fty_value = dct_old_prev_fty_rows['PASS_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 100
                dct_old_prev_fty_value = round(dct_old_prev_fty_value, 1)
                dct_old_prev_dpt_value = dct_old_prev_fty_rows['REJECT_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 1000
                dct_old_prev_dpt_value = int(round(dct_old_prev_dpt_value, 0))


                dct_old_curr_fty_value = 0
                dct_old_curr_dpt_value = 0
                dct_old_major_faults = 0000
                final_remarks = 0000

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

                dct_old_prev_fty_value=0
                dct_old_prev_dpt_value=0

                dct_old_curr_fty_value = (last_month_df_dct_old['PASS_QUANTITY'].sum() / last_month_df_dct_old['TEST_QUANTITY'].sum()) * 100
                dct_old_curr_fty_value = round(dct_old_curr_fty_value, 1)
                dct_old_curr_dpt_value = (last_month_df_dct_old['REJECT_QUANTITY'].sum() / last_month_df_dct_old[
                    'TEST_QUANTITY'].sum()) * 1000

                dct_old_curr_dpt_value = (round(dct_old_curr_dpt_value, 0))

                df_dct_old_raw2=df_raw[df_raw['MONTH']==month_order[0]]
                df_dct_old_raw2 = df_raw[df_raw['DESIGN'] == des]
                pending_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'PENDING']

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
                                                                "WRONG MOUNT", "REVERSE POLARITY", "SOLDER SHORT",
                                                                "WRONG COMPONENT"])]
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

                    comp_per_product = {}
                    for i in comp_group['PRODUCT_NAME']:
                        result_value2 = comp_group.loc[comp_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        comp_per_product[i] = result_value2

                    assembly_remarks = []
                    for i in assemble_per_product.keys():
                        if assemble_per_product[i] > assembly_40:
                            assembly_remarks.append(f'{i}[{str(assemble_per_product[i])}]')


                    sorted_assembly_remarks = sorted(assembly_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                     reverse=True)

                    assembly_remarks_str = ', '.join(sorted_assembly_remarks)

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

                df_dct_old_prev = df_dct_old.groupby(["MONTH"]).sum(numeric_only=True).reset_index()

                df_dct_old_curr = df_dct_old.groupby(["MONTH", 'PRODUCT']).sum(numeric_only=True).reset_index()

                partcodes = df_dct_old_curr['PRODUCT'].unique()


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

                    if (count <= 1) and (df_dct_old_curr['MONTH'] == month_order[-1]).any():

                        rows_to_remove = df_dct_old_curr[df_dct_old_curr['PRODUCT'] == i]
                        df_dct_old_curr = df_dct_old_curr.drop(rows_to_remove.index)

                dct_old_prev_fty_rows = df_dct_old_prev.iloc[:-1]
                dct_old_prev_fty_value = dct_old_prev_fty_rows['PASS_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 100
                dct_old_prev_fty_value = round(dct_old_prev_fty_value, 1)
                dct_old_prev_dpt_value = dct_old_prev_fty_rows['REJECT_QUANTITY'].sum() / dct_old_prev_fty_rows[
                    'TEST_QUANTITY'].sum() * 1000
                dct_old_prev_dpt_value = int(round(dct_old_prev_dpt_value, 0))


                dct_old_curr_fty_row = df_dct_old_prev.tail(1)

                dct_old_curr_fty_value = dct_old_curr_fty_row['PASS_QUANTITY'] / dct_old_curr_fty_row['TEST_QUANTITY'] * 100

                dct_old_curr_fty_value = round(dct_old_curr_fty_value, 1)
                dct_old_curr_dpt_value = dct_old_curr_fty_row['REJECT_QUANTITY'] / dct_old_curr_fty_row[
                    'TEST_QUANTITY'] * 1000

                dct_old_curr_dpt_value = int(round(dct_old_curr_dpt_value, 0))



                # Categories

                fc = ['DRY SOLDER', 'COMP. FAIL', 'COMP. DMG/MISS', 'SOLDER SHORT', 'SCRAP', 'MAGNETICS ISSUE',
                      'UNDER ANALYSIS', 'CC ISSUE', 'WRONG COMPONENT',
                      'WRONG MOUNT', 'RETEST', 'REVERSE POLARITY', 'COATING ISSUE', 'Other', 'OPERATOR FAULT',
                      'NOT FOUND', 'CALIBRATION ISSUE', 'PROG. ISSUE', 'LEAD CUT ISSUE', 'ECN NOT DONE']
                fault_cat = {}
                for i in fc:
                    fault_cat[i] = 0

                # DCT OLD REMARKS
                ################################################################################################################3#####

                df_dct_old_raw = df_raw[(df_raw['DESIGN'] == des)]
                df_dct_old_raw['MONTH'] = pd.Categorical(df_dct_old_raw['MONTH'], categories=month_order, ordered=True)
                products_to_keep = df_dct_old_curr['PRODUCT'].unique()

                df_dct_old_raw = df_dct_old_raw[
                    df_dct_old_raw['PRODUCT_NAME'].isin(products_to_keep)]  # all months without pilot
                df_dct_old_wo_latest_month = df_dct_old_raw[
                    df_dct_old_raw['MONTH'] != month_order[-1]]  # all months without pilot without last

                df_dct_old_raw2 = df_dct_old_raw[df_dct_old_raw['MONTH'] == month_order[-1]]  # latest month without pilot


                # Pending
                pending_df = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'] == 'PENDING']

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


                # formatted_list = [f"{item[0]}({item[1]}) - {item[2]}" for item in new_fault]

                blank_dict = {}
                for i in new_fault:

                    if i[0] in blank_dict.keys():
                        blank_dict[i[0]] = blank_dict[i[0]] + [(i[2])]
                    else:
                        blank_dict[i[0]] = [i[2]]


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


                total_faults_dct_old = len(df_dct_old_raw2)

                flag = True

                if total_faults_dct_old == 0:

                    flag = False  # 0 faults of latest month

                assembly = df_dct_old_raw2[
                    df_dct_old_raw2['FAULT_CATEGORY'].isin(['DRY SOLDER', "SOLDER SHORT", "COMP. DMG/MISS",
                                                            "WRONG MOUNT", "REVERSE POLARITY", "SOLDER SHORT",
                                                            "WRONG COMPONENT"])]

                if flag:


                    assembly_qty = len(assembly)
                    assembly_prcnt = assembly_qty / total_faults_dct_old * 100
                    assembly_prcnt = int(round(assembly_prcnt, 0))


                    comp = df_dct_old_raw2[df_dct_old_raw2['FAULT_CATEGORY'].isin(["COMP. FAIL", "CC ISSUE"])]
                    comp_qty = len(comp)
                    comp_prcnt = comp_qty / total_faults_dct_old * 100
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

                    comp_per_product = {}
                    for i in comp_group['PRODUCT_NAME']:
                        result_value2 = comp_group.loc[comp_group['PRODUCT_NAME'] == i, 'STAGE'].values[0]
                        comp_per_product[i] = result_value2

                    assembly_remarks = []
                    for i in assemble_per_product.keys():
                        if assemble_per_product[i] > assembly_40:
                            assembly_remarks.append(f'{i}[{str(assemble_per_product[i])}]')

                    sorted_assembly_remarks = sorted(assembly_remarks, key=lambda x: int(x.split('[')[1][:-1]),
                                                     reverse=True)

                    assembly_remarks_str = ', '.join(sorted_assembly_remarks)

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

                #     dct_old_curr_fty_value = 0
                #     dct_old_curr_dpt_value = 0
                #     dct_old_major_faults = 0000
                #     final_remarks = 0000

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




        if len(month_order)==1:
            df= df.drop(df.columns[1], axis=1)
            df = df.drop(df.columns[1], axis=1)
        elif len(month_order)==2:

            rename_fx = ['Design', f'FTY%-({month_order[0]})', f'DPT-({month_order[0]})', f'FTY%-({month_order[-1]})',
                         f'DPT-({month_order[-1]})', 'Major Faults', 'Remarks']

            # Rename the columns using the rename_fx list
            df.columns = rename_fx
        elif len(month_order)> 2:

            rename_fx = ['Design', f'FTY%-({month_order[0]}-{month_order[-2]})', f'DPT-({month_order[0]}-{month_order[-2]})', f'FTY%-({month_order[-1]})',
                         f'DPT-({month_order[-1]})', 'Major Faults', 'Remarks']

            # Rename the columns using the rename_fx list
            df.columns = rename_fx
        final_df = df

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



            df_remarks = df10.pivot_table(index=["FAULT_OBSERVED", "FAULT_CATEGORY"], values='KEY_COMPONENT',
                                          aggfunc=lambda x: ", ".join(str(v) for v in x.unique())).fillna(
                '').reset_index()

            df11 = df11.rename(columns={"COMPONENT DAMAGE": "Dmg",
                                        "COMPONENT MISSING": "Miss",
                                        "Component faulty": "faulty",
                                        "FAULT_OBSERVED": "Faults",
                                        "REVERSE POLARITY": "Polarity",
                                        "SOLDER SHORT": "Solder Short", "DRY SOLDER": "Dry Solder", "SCRAP": "Scrap",
                                        "MAGNETICS ISSUE": "Magnetics",
                                        "COMP. FAIL": "Comp. Fail",
                                        "COMP. DMG/MISS": "Comp. Dmg/Miss", "PENDING": "Pending",
                                        "WRONG COMPONENT": "Wrong Component",
                                        "WRONG MOUNT": "Wrong Mount", "CC ISSUE": "CC Issue", "RETEST": "Retest"})
            df11["Faults"] = df11["Faults"].str.upper()

            df15 = (df_raw[df_raw["PRODUCT_NAME"] == m]).reset_index()

            list_of_partcode = df15.PART_CODE.unique()
            str_of_partcode = ', '.join(list_of_partcode)
            list_of_month1 = df15.MONTH.unique()


            bar = pd.crosstab(df10["FAULT_OBSERVED"], df10["STAGE"])
            bar = bar.reset_index()
            bar = bar.rename(columns={"FAULT_OBSERVED": "Faults"})

            # bar['Faults'] = df_raw['Faults'].str.upper()

            category = pd.crosstab(df10["FAULT_OBSERVED"], df10["FAULT_CATEGORY"])
            category1_df = pd.DataFrame(category)

            category = category.reset_index()

            category.drop(["FAULT_OBSERVED"], axis=1, inplace=True)
            category.loc['Total'] = category.sum(axis=0)

            category = category.tail(1)


            lis_col_names = list(category.columns.values)
            lis_col_names_all = ["CC ISSUE", "COMP. FAIL", "COMP. DMG/MISS", "MAGNETICS ISSUE",
                                 "DRY SOLDER", "SCRAP", "SOLDER SHORT", "WRONG MOUNT", "REVERSE POLARITY",
                                 "PENDING", "WRONG COMPONENT",
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
                                                "WRONG COMPONENT": "Wrong Component",
                                                "WRONG MOUNT": "Wrong Mount", "RETEST": "Retest", "SCRAP": "Scrap"})

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
                                                  "WRONG COMPONENT": "Wrong Component",
                                                  "WRONG MOUNT": "Wrong Mount", "RETEST": "Retest"})

            final_df2 = final_df2.replace(np.nan, 0)

            # Convert all columns to integer data type
            final_df2 = final_df2.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)

            final_df2["Remarks"] = " "
            # total line of summary
            final_df2.at[len(final_df2), 'Faults'] = "Total"
            final_df2.at[len(final_df2) - 1, 'Total'] = str(total_sum) + "/" + str(total_sum)
            final_df2.at[len(final_df2) - 1, '%age'] = str(100) + "%"

            for column in ["CC Issue", "Polarity", "Solder Short", "Comp. Fail", "Comp. Dmg/Miss", "Magnetics",
                           "Dry Solder", "Wrong Mount", "Retest", "LEAD CUT ISSUE", "PROGRAMMING ISSUE", "Scrap",
                           "Pending", "Wrong Component",
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