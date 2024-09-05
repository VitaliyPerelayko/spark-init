from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import udf, lit, count, count_distinct, when, sum, reduce, collect_list, \
    named_struct
from pyspark.sql.types import DecimalType


def aggregate_data(part_date: str,
                   dm_currency: str,
                   transactions: DataFrame,
                   articles: DataFrame,
                   customers: DataFrame
                   ):
    @udf(returnType=DecimalType(19, 17))
    def calc_dm_price(exchange_rates: str, price: str, currency: str) -> Decimal:
        if dm_currency == currency:
            return Decimal(price)
        exchange_dict = eval(exchange_rates.upper())
        return Decimal(price) * Decimal(exchange_dict[dm_currency])

    month = part_date[:7]

    month_transactions = transactions.filter(transactions.t_dat.startswith(month))
    month_transactions = month_transactions.select(
        month_transactions.t_dat,
        month_transactions.customer_id,
        month_transactions.article_id,
        month_transactions.currency,
        calc_dm_price(month_transactions.current_exchange_rate,
                      month_transactions.price,
                      month_transactions.currency)
        .alias('dm_price')
    )

    month_transactions = month_transactions.join(
        articles,
        month_transactions.article_id == articles.id,
        'left'
    )

    def max_purchase(purchase1: Column, purchase2: Column) -> Column:
        return (when(purchase1.price > purchase2.price, purchase1)
                .when(((purchase1.price == purchase2.price) & (purchase1.t_dat <= purchase2.t_dat)), purchase1)
                .otherwise(purchase2)
                )

    aggregated_transactions = (
        month_transactions
        .groupBy(month_transactions.customer_id)
        .agg(
            count(lit(1)).alias('number_of_articles'),
            sum(month_transactions.dm_price).alias('transaction_amount'),
            count_distinct(month_transactions.product_group_name).alias('number_of_product_groups'),
            reduce(
                col=collect_list(
                    named_struct(
                        lit('price'), month_transactions.dm_price,
                        lit('article_id'), month_transactions.article_id,
                        lit('t_dat'), month_transactions.t_dat
                    )),
                initialValue=named_struct(
                    lit('price'), lit('0.0').cast(DecimalType(19, 17)),
                    lit('article_id'), lit('initial'),
                    lit('t_dat'), lit(part_date)
                ),
                merge=max_purchase,
                finish=lambda purchase: purchase.article_id
            ).alias('most_exp_article_id')
        )
    )

    aggregated_transactions = aggregated_transactions.join(
        customers,
        customers.id == aggregated_transactions.customer_id,
        'left'
    )

    aggregated_transactions = (
        aggregated_transactions
        .withColumn('dm_currency', lit(dm_currency))
        .withColumn('part_date', lit(part_date)))

    data_mart = aggregated_transactions.select(
        aggregated_transactions.part_date,
        aggregated_transactions.customer_id,
        aggregated_transactions.customer_group_by_age,
        aggregated_transactions.transaction_amount,
        aggregated_transactions.most_exp_article_id,
        aggregated_transactions.number_of_articles,
        aggregated_transactions.number_of_product_groups,
        aggregated_transactions.dm_currency,
    )

    return data_mart
