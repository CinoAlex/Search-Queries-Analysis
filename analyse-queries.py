
# coding: utf-8

# In[426]:

import pandas as pd
import numpy as np
import os
import csv
output_df = pd.DataFrame(columns=['keyword', 'clicks'])
with open('input_data.csv') as f:
    rows = csv.reader(f)
    for row in rows:
        keyword_column = row[0]
        clicks_column = int(row[1])
        cost_column = float(row[2])
        transactions_column = int(row[3])
        keywords = keyword_column.split()
        for word in keywords:
            output_df = output_df.append({'keyword': word, 'clicks': clicks_column,'cost': cost_column, 'transactions': transactions_column}, ignore_index=True)
output_df.clicks = output_df.clicks.astype('int64', copy=False)
output_df.transactions = output_df.transactions.astype('int64', copy=False)
output_df.keyword = output_df.keyword.str.lower()
final_df = output_df.groupby("keyword").sum()
final_df.reset_index(inplace=True)
final_output_result_no_convs = final_df[final_df.transactions == 0]
final_output_result_no_convs.to_csv('1-lookup_words_no_convs.csv', index = False)
with open('1-lookup_words_no_convs.csv', 'r') as file:
  reader = csv.reader(file)
  lookup_words_no_convs = list(reader)
lookup_words_no_convs_words = [item[0] for item in lookup_words_no_convs]
my_ads_pool = pd.read_csv('input_data.csv',names = ["keyword_phrases", "clicks", "cost", "transactions", "bounce_rate"])
my_ads_pool['bounce_rate'] = pd.to_numeric(my_ads_pool['bounce_rate'].astype(str).str.strip('%'), errors='coerce')
def getKeywordsFrom(keywords):
    result = ""
    for keyword in keywords.split():
        for lookup_keyword in lookup_words_no_convs_words:
            if keyword == lookup_keyword:
                result = result + " " + keyword
    return result
my_ads_pool['bad_to_ok_keywords'] = my_ads_pool['keyword_phrases'].apply(
    lambda keywords: getKeywordsFrom(keywords)
)
fitered_df = my_ads_pool.sort_values(by='bad_to_ok_keywords')
final_no_convs = fitered_df[fitered_df.bad_to_ok_keywords.str.contains(' ')]
final_df_avg = final_no_convs.groupby("bad_to_ok_keywords").mean().drop(['clicks', 'cost', 'transactions'], axis=1)
final_df_avg.reset_index(inplace=True)
final_df_avg_sorted = final_df_avg.sort_values(by='bad_to_ok_keywords', ascending=True)
final_df_sum = final_no_convs.groupby("bad_to_ok_keywords").sum().drop(['bounce_rate'], axis = 1)
final_df_sum.reset_index(inplace=True)
final_df_sum_sorted = final_df_sum.sort_values(by='bad_to_ok_keywords', ascending=True)
final_sums_and_avgs = pd.merge(final_df_sum_sorted, final_df_avg_sorted, on='bad_to_ok_keywords')
final_sums_and_avgs.columns = ['bad_to_ok_keywords', 'clicks_sum',  'cost_sum', 'transactions_sum', 'bounce_rate_avg']
final_sums_and_avgs['category'] = np.where(final_sums_and_avgs['bounce_rate_avg']<100, 'ok', 'bad')
final_sums_and_avgs['bounce_rate_avg'] = pd.to_numeric(final_sums_and_avgs['bounce_rate_avg'].astype(int))
final_no_convs_pool = final_no_convs.reset_index(drop=True)
final_result = pd.merge(final_no_convs_pool, final_sums_and_avgs, on='bad_to_ok_keywords')
final_result_drop_columns = final_result.drop(['transactions_sum','bounce_rate'], axis=1)
final_result_drop_columns.to_csv('2-ppc_bad_to_ok.csv', index = False)
inputData = pd.read_csv('input_data.csv',names = ["keyword_phrases", "clicks", "cost", "transactions", "bounce_rate"])
inputData_with_convs = inputData[(inputData[['transactions']] != 0).all(axis=1)]
searchConsole = pd.read_csv('search_console_data.csv',names = ["keyword_phrases", "clicks", "impressions", "ctr", "position"])
vlookup = pd.merge(inputData_with_convs, searchConsole, on='keyword_phrases')
vlookup_new = vlookup.drop(['clicks_y','impressions','ctr','position'], axis=1)
vlookup_new.columns = ['keyword_phrases','clicks', 'cost','transactions','bounce_rate']
vlookup_new.to_csv('3-converted_found_in_search_console.csv', index = False)
joined_frames = vlookup_new.append(inputData_with_convs).sort_values(by='keyword_phrases', ascending=False)
not_found_in_search_console_but_converted = joined_frames.drop_duplicates(keep=False)
not_found_in_search_console_but_converted.to_csv('4-converted_not_found_in_search_console.csv', index = False)


# In[ ]:




# In[ ]:




# In[ ]:



