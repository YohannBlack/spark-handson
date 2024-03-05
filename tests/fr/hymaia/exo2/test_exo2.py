from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.main import filter, join_zip_city, department_from_zip
from src.fr.hymaia.exo2.agregate import aggregate
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_filter(self):
        input = spark.createDataFrame(
            [
                Row(age=18, name='toto'),
                Row(age=17, name='tata')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(age=18, name='toto')
            ]
        )

        actual = filter(input)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_join_zip_city(self):
        input1 = spark.createDataFrame(
            [
                Row(name='tota', age=63, zip=75001),
                Row(name='toto', age=23, zip=91400),
            ]
        )
        input2 = spark.createDataFrame(
            [
                Row(zip=75001, city='Paris'),
                Row(zip=91400, city='Orsay')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(zip=75001, name='tota', age=63, city='Paris'),
                Row(zip=91400, name='toto', age=23, city='Orsay'),
            ]
        )

        actual = join_zip_city(input1, input2)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_department_from_zip(self):
        input = spark.createDataFrame(
            [
                Row(name='tota', age=63, zip=75001),
                Row(name='toto', age=23, zip=91400),
                Row(name='titi', age=23, zip=20190),
                Row(name='tutu', age=23, zip=20600),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='tota', age=63, zip=75001, department='75'),
                Row(name='toto', age=23, zip=91400, department='91'),
                Row(name='titi', age=23, zip=20190, department='2A'),
                Row(name='tutu', age=23, zip=20600, department='2B'),
            ]
        )

        actual = department_from_zip(input)

        self.assertCountEqual(actual.collect(), expected.collect())

    # def test_department_from_zip_invalid_values(self):
    #     input = spark.createDataFrame(
    #         [
    #             Row(name='toto', age=23, zip='20A15')
    #         ]
    #     )

    #     expected = department_from_zip(input)
    #     print(expected)
    #     self.assertIsNone(expected)
