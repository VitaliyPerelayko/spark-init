from typing import Callable

from pyspark import RDD


# CUSTOMERS
def customer_line_to_tuple(line: str) -> tuple:
    cols = line.split(',')
    c_id = str(cols[0])
    c_age = int(cols[5] if cols[5] != '' else 0)
    if c_age < 23:
        age_group = 'S'
    elif c_age > 59:
        age_group = 'R'
    else:
        age_group = 'A'
    return c_id, age_group


# ARTICLES

def article_line_to_tuple(line: str) -> tuple:
    cols = line.split(',')
    a_id = str(cols[0])
    a_product_group = str(cols[5])
    return a_id, a_product_group


# TRANSACTIONS_TRAIN
def transaction_line_to_tuple(line: str) -> tuple:
    cols = line.split(',')
    c_id = str(cols[1])
    a_id = str(cols[2])
    price = float(cols[3])
    t_date = cols[0]
    return a_id, (c_id, price, t_date)


def parse_raw_text(raw_rdd: RDD, parser: Callable) -> RDD:
    header = raw_rdd.first()
    return raw_rdd.filter(lambda line: line != header).map(parser)


def aggregator_reducer(record1: tuple, record2: tuple) -> tuple:
    def most_exp_article_id(item1: tuple, item2: tuple) -> tuple:
        # price
        if item1[1] > item2[1]:
            return item1
        # if prices are equal get the earliest purchase
        if item1[1] == item2[1] and item1[2] <= item2[2]:
            return item1

        return item2

    # (customer_id, (article_id, price, transaction_date), 1, transaction_date, {product_group})
    most_exp = most_exp_article_id(record1[0], record2[0])
    counter = record1[1] + record2[1]
    summ = record1[2] + record2[2]
    product_groups = record1[3]
    if product_groups is None:
        product_groups = set()
    if record2[3] is not None:
        product_groups.update(record2[3])
    return most_exp, counter, summ, product_groups


def map_data_for_aggregate_reducer(entity: tuple) -> tuple:
    """
    map structure (article_id, ((customer_id, price, transaction_date), product_group)
    to (customer_id, ((article_id, price, transaction_date), 1, transaction_date, {product_group}))
    {product_group} for collecting distinct product groups
    1 for counting total number of transactions
    """
    customer_id = entity[1][0][0]
    article_id = entity[0]
    price = entity[1][0][1]
    transaction_date = entity[1][0][2]
    product_group = entity[1][1]

    return customer_id, (article_id, price, transaction_date), 1, transaction_date, {product_group}


def map_to_final_view(entity: tuple) -> tuple:
    """
    map structure
    (customer_id, ((article_id, price, transaction_date), n, transaction_date, {product_group1, product_group2 ...}))
    to
    (customer_id, (transaction_amount, most_exp_article_id, number_of_articles, number_of_product_groups))
    """
    customer_id = entity[0]
    transaction_amount = entity[1][2]
    most_exp_article_id = entity[1][0][0]
    number_of_articles = entity[1][1]
    number_of_product_groups = 0 if entity[1][3] is None else len(entity[1][3])
    return customer_id, (transaction_amount, most_exp_article_id, number_of_articles, number_of_product_groups)


def build_data_mart(customers: RDD, articles: RDD, transaction_train: RDD, part_date: str):
    month = part_date[:7]
    customers_rdd = parse_raw_text(customers, customer_line_to_tuple)
    articles_rdd = parse_raw_text(articles, article_line_to_tuple)
    transactions_rdd = (
        transaction_train.filter(lambda line: line.startswith(month)).
        map(transaction_line_to_tuple))

    result_rdd = (
        transactions_rdd.
        leftOuterJoin(articles_rdd, 12).
        # (45, (('customer_85', 0.8862806950662577, date), 'Accessories'))
        map(map_data_for_aggregate_reducer).
        # ('customer_85', ((45, 0.8862806950662577, date), 1, 0.8862806950662577, {'Accessories'}))
        reduceByKey(aggregator_reducer, 12).
        # ('customer_9', ((2, 0.7088229847122839, '2018-12-01'), 7, 3.2343342063844602, {'Garment Lower body', 'Garment Upper body', 'Socks & Tights'}))
        map(map_to_final_view).
        # ('customer_id', (transaction_amount, most_exp_article_id, number_of_articles, number_of_product_groups)
        leftOuterJoin(customers_rdd, 12).
        # ('customer_9', ((2.214346991115221, 4, 6, 4), 'R'))
        map(
            lambda t: f'{part_date},{t[0]},{t[1][1]},{t[1][0][0]},{t[1][0][1]},{t[1][0][2]},{t[1][0][3]}')
        # 'part_date',customer_id,customer_group_by_age,transaction_amount,most_exp_article_id,number_of_articles,number_of_product_groups
    )

    return result_rdd
