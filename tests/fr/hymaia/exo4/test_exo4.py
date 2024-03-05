from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo4.no_udf import determine_category, add_column_price_per_day, add_column_price_cumul_last_30_days


class TestMain(unittest.TestCase):

    def test_determine_category(self):
        input = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01', price=10),
                Row(category=6, date='2020-01-01', price=20)
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01',
                    price=10, category_name='food'),
                Row(category=6, date='2020-01-01',
                    price=20, category_name='furniture')
            ]
        )

        actual = determine_category(input)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)

    def test_add_column_price_per_day(self):
        input = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01',
                    price=10, category_name='food'),
                Row(category=5, date='2020-01-01',
                    price=20, category_name='food'),
                Row(category=6, date='2020-01-01',
                    price=30, category_name='furniture')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01', price=10,
                    category_name='food', total_price_per_category_per_day=30),
                Row(category=5, date='2020-01-01', price=20,
                    category_name='food', total_price_per_category_per_day=30),
                Row(category=6, date='2020-01-01', price=30,
                    category_name='furniture', total_price_per_category_per_day=30)
            ]
        )

        actual = add_column_price_per_day(input)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)

    def test_add_column_price_cumul_last_30_days(self):
        input = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01',
                    price=10, category_name='food'),
                Row(category=5, date='2020-01-02',
                    price=20, category_name='food'),
                Row(category=6, date='2020-01-01',
                    price=30, category_name='furniture')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(category=5, date='2020-01-01', price=10, category_name='food',
                    total_price_per_category_per_day_last_30_days=10),
                Row(category=5, date='2020-01-02', price=20, category_name='food',
                    total_price_per_category_per_day_last_30_days=30),
                Row(category=6, date='2020-01-01', price=30, category_name='furniture',
                    total_price_per_category_per_day_last_30_days=30)
            ]
        )

        actual = add_column_price_cumul_last_30_days(input)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)
