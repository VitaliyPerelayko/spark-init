from datetime import date, timedelta

import pandas as pd

DATA_FOLDER = '/home/user/senla/ds_engineer/dataset'
MIN_PART_DATE = date.fromisoformat('2018-09-30')
LOYALITY_PARAM = 2

part_date = '2018-12-31'

# ARTICLES
article_columns = {
    'article_id': 'string',
    'product_group_name': 'category'
}
articles = pd.read_csv(
    filepath_or_buffer=DATA_FOLDER + '/articles.csv',
    usecols=list(article_columns.keys()),
    dtype=article_columns
)

# CUSTOMERS
customer_columns = {
    'customer_id': 'string',
    'age': 'Int64'
}

customers = pd.read_csv(
    filepath_or_buffer=DATA_FOLDER + '/customers.csv',
    usecols=list(customer_columns.keys()),
    dtype=customer_columns
)

customers['age'] = customers['age'].fillna(int(customers['age'].mean()))
# categorize age 0 <= S <= 22, 23 <= A <= 59, 60 <= R <=
customers['customer_group_by_age'] = pd.cut(
    x=customers['age'],
    labels=['S', 'A', 'R'],
    bins=[-1, 22, 59, 500]
)
customers = customers[['customer_id', 'customer_group_by_age']]

# TRANSACTIONS
previous_month = []
previous_month_date = date.fromisoformat(part_date).replace(day=1) - timedelta(days=1)
loyality_month = LOYALITY_PARAM
while previous_month_date > MIN_PART_DATE and loyality_month > 0:
    previous_month.append(previous_month_date.isoformat()[:7])
    previous_month_date = previous_month_date.replace(day=1) - timedelta(days=1)
    loyality_month -= 1

# calculate how many month customer should continuously buy smth
# to considered as loyal
active_month_for_loyality = LOYALITY_PARAM - loyality_month

transactions_columns = {
    't_dat': 'string',
    'customer_id': 'string',
    'article_id': 'string',
    'price': 'Float64'
}

all_transactions = pd.read_csv(
    filepath_or_buffer=DATA_FOLDER + '/transactions_train.csv',
    usecols=list(transactions_columns.keys()),
    dtype=transactions_columns
)

# get customer loyality for previous month
previous_month_transactions = all_transactions[all_transactions['t_dat'].str.startswith(tuple(previous_month))]
previous_month_transactions['month'] = previous_month_transactions['t_dat'].str.split('-').str[1]
prev_month_loyality = previous_month_transactions.groupby('customer_id')['month'].nunique().reset_index()

# target month transactions (for specified part_date)
transactions = all_transactions[all_transactions['t_dat'].str.startswith(part_date[:7])]

transactions_articles = transactions.merge(right=articles, on='article_id', how='left')


def aggregation(data: pd.DataFrame) -> pd.Series:
    import numpy as np

    dates = data['t_dat'].to_numpy(np.str_)
    prices = data['price'].to_numpy()
    article_ids = data['article_id'].to_numpy(np.str_)

    decade_sum = np.array([0, 0, 0], dtype=np.float64)

    def get_decade(transaction_date: np.str_):
        month_day = transaction_date[-2:]
        if '01' <= month_day <= '10':
            return 0
        if '11' <= month_day <= '20':
            return 1
        if '21' <= month_day <= '31':
            return 2

    decades = np.vectorize(get_decade)(dates)

    def reducer():
        max_price_index = 0
        max_price = 0
        for i, value in np.ndenumerate(prices):
            index = i[0]
            decade = decades[index]
            decade_sum[decade] += value
            if value > max_price or (
                    value == max_price and
                    dates[index] < dates[max_price_index]):
                max_price = value
                max_price_index = index
        return article_ids[max_price_index]

    most_exp_article = reducer()

    if decade_sum[0] >= decade_sum[1]:
        if decade_sum[0] >= decade_sum[2]:
            most_active_decade = 1
        else:
            most_active_decade = 3
    else:
        if decade_sum[1] >= decade_sum[2]:
            most_active_decade = 2
        else:
            most_active_decade = 3

    agg = {
        'transaction_amount': decade_sum.sum(),
        'most_exp_article_id': most_exp_article,
        'number_of_articles': len(data.index),
        'number_of_product_groups': data['product_group_name'].nunique(),
        'most_active_decade': most_active_decade
    }
    return pd.Series(agg, list(agg.keys()))


aggregated_transactions = (transactions_articles
                           .groupby('customer_id')
                           .apply(func=aggregation, include_groups=False)
                           .reset_index())

customer_statististic = aggregated_transactions.merge(
    right=customers, on='customer_id', how='left'
)

customer_statististic = customer_statististic.merge(
    right=prev_month_loyality, on='customer_id', how='left'
)

customer_statististic['customer_loyalty'] = (
    (customer_statististic['month'] == active_month_for_loyality).astype(int))

customer_statististic['part_date'] = part_date
data_mart = customer_statististic[[
    'part_date',
    'customer_id',
    'customer_group_by_age',
    'transaction_amount',
    'most_exp_article_id',
    'number_of_articles',
    'number_of_product_groups',
    'most_active_decade',
    'customer_loyalty'
]]

data_mart.to_csv(path_or_buf=f'{DATA_FOLDER}/data_mart_{part_date}.csv', index=False)
