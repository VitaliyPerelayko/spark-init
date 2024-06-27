from datetime import date, timedelta
from decimal import Decimal
from os import getenv

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import udf, lit, count, count_distinct, when, sum, reduce, collect_list, \
    named_struct, mode, col
from pyspark.sql.types import DecimalType

RESULT_FOLDER = str(getenv('RESULTS_FOLDER'))


def aggregate_data(part_date: str,
                   dm_currency: str,
                   loyality_level: int,
                   transactions: DataFrame,
                   articles: DataFrame,
                   customers: DataFrame
                   ):
    @udf(returnType=DecimalType(22, 17))
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
            mode(month_transactions.product_group_name).alias('most_freq_product_group_name'),
            reduce(
                col=collect_list(
                    named_struct(
                        lit('price'), month_transactions.dm_price,
                        lit('article_id'), month_transactions.article_id,
                        lit('t_dat'), month_transactions.t_dat
                    )),
                initialValue=named_struct(
                    lit('price'), lit('0.0').cast(DecimalType(22, 17)),
                    lit('article_id'), lit('initial'),
                    lit('t_dat'), lit(part_date)
                ),
                merge=max_purchase,
                finish=lambda purchase: purchase.article_id
            ).alias('most_exp_article_id')
        )
    )

    transactions_customers = aggregated_transactions.join(
        customers,
        customers.id == aggregated_transactions.customer_id,
        'left'
    )

    # calc loyal_months_nr
    if loyality_level == 1:
        transactions_customers = transactions_customers.withColumn('loyal_months_nr', lit(1))
    else:
        prev_part_date = date.fromisoformat(part_date).replace(day=1) - timedelta(days=1)
        filename = RESULT_FOLDER + '/data_mart_v3_' + prev_part_date.isoformat()
        prev_month_loyalty = (
            transactions_customers.sparkSession
            .read.parquet(filename)
            .select(
                col('customer_id').alias('pml_customer_id'),
                col('loyal_months_nr').alias('pml_loyal_months_nr')
            ))
        transactions_customers = transactions_customers.join(
            prev_month_loyalty,
            prev_month_loyalty.pml_customer_id == transactions_customers.customer_id,
            'left'
        )

        transactions_customers = transactions_customers.withColumn(
            'loyal_months_nr',
            when(transactions_customers.pml_loyal_months_nr.isNull(), 1)
            .otherwise(transactions_customers.pml_loyal_months_nr + 1)
        )

    # calc customer_loyality
    data_mart = transactions_customers.withColumn(
        'customer_loyality',
        when(transactions_customers.loyal_months_nr >= loyality_level, 1)
        .otherwise(0)
    )

    # calc offer
    data_mart = data_mart.withColumn(
        'offer',
        when(
            (data_mart.customer_loyality == 1) &
            (data_mart.club_member_status == 'ACTIVE') &
            (data_mart.fashion_news_frequency == 'Regularly'),
            1)
        .otherwise(0)
    )

    data_mart = (
        data_mart
        .withColumn('dm_currency', lit(dm_currency))
        .withColumn('part_date', lit(part_date)))

    data_mart = data_mart.select(
        data_mart.part_date,
        data_mart.customer_id,
        data_mart.customer_group_by_age,
        data_mart.transaction_amount,
        data_mart.dm_currency,
        data_mart.most_exp_article_id,
        data_mart.number_of_articles,
        data_mart.number_of_product_groups,
        data_mart.most_freq_product_group_name,
        data_mart.loyal_months_nr,
        data_mart.customer_loyality,
        data_mart.offer
    )

    return data_mart

