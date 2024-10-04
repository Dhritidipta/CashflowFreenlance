import unittest
from pyspark.sql import SparkSession
import pandas as pd
import sys
sys.path.append("C:/Work/Freelancing/1/CashFlow")
from data.fetch import get_all_companies_df, get_exchange_rates_df, get_all_sepa_transactions_df, get_all_swift_transactions_df

class TestDataProcessing(unittest.TestCase):

    def test_spark_session(self):
        # Initialize a Spark session
        spark = SparkSession.builder \
            .appName("SparkSessionTest") \
            .getOrCreate()

        # Check if the Spark session is created successfully
        self.assertIsNotNone(spark, "Spark session should not be None")
        self.assertTrue(spark.getActiveSession(), "Spark context should be running")

        # Stop the Spark session after testing
        spark.stop()

    def test_company_data(self):
        # Test if company data is being fetched correctly as a pandas DataFrame
        df = get_all_companies_df()
        self.assertIsInstance(df, pd.DataFrame, "Expected a pandas DataFrame")
        self.assertGreater(len(df), 0, "Company DataFrame has less than 1000 rows")
        self.assertIn("id", df.columns, "company_id column missing")

    def test_exchange_rates(self):
        # Test if exchange rates data is fetched properly as a pandas DataFrame
        df = get_exchange_rates_df()
        self.assertIsInstance(df, pd.DataFrame, "Expected a pandas DataFrame")
        self.assertGreater(len(df), 0, "Exchange Rates DataFrame is empty")
        self.assertIn("currency", df.columns, "Currency column missing")

    def test_sepa_transaction_data(self):
        # Test if SEPA transaction data is being fetched correctly as a pandas DataFrame
        df = get_all_sepa_transactions_df()
        self.assertIsInstance(df, pd.DataFrame, "Expected a pandas DataFrame")
        self.assertGreater(len(df), 0, "SEPA Transactions DataFrame is empty")
        self.assertIn("id", df.columns, "Transaction ID column missing")

    def test_swift_transaction_data(self):
        # Test if SEPA transaction data is being fetched correctly as a pandas DataFrame
        df = get_all_swift_transactions_df()
        self.assertIsInstance(df, pd.DataFrame, "Expected a pandas DataFrame")
        self.assertGreater(len(df), 0, "SWIFT Transactions DataFrame is empty")
        self.assertIn("id", df.columns, "Transaction ID column missing")


# Custom test runner to print passed/failed tests
class CustomTestRunner(unittest.TextTestRunner):
    def run(self, test):
        result = super().run(test)
        print(f"\n{result.testsRun} tests run")
        print(f"{len(result.failures)} failures")
        print(f"{len(result.errors)} errors")
        print(f"{result.testsRun - len(result.failures) - len(result.errors)} successes")
        return result
# Run the tests
if __name__ == '__main__':
    runner = CustomTestRunner(verbosity=2)
    unittest.main(testRunner=runner, argv=[''], exit=False)
