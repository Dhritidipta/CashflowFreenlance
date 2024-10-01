import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Data Fetch") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
# schema = StructType([     StructField("ID", IntegerType(), True),     StructField("Name", StringType(), True),     StructField("Age", IntegerType(), True),     StructField("Salary", FloatType(), True), ]) 
# # Step 3:
# data = [     (1, "John Doe", 30, 50000.00),     (2, "Jane Smith", 25, 60000.00),     (3, "Sam Brown", 40, 70000.00), (4, "Lisa Ray", 35, 65000.00) ]
 
# # Step 4: Create DataFrame from Dummy Data
# df = spark.createDataFrame(data, schema)
# # Step 5: Show DataFrame
# df.show()


# # API endpoint and initial parameters
# # url = 'http://localhost:9090/companies'
# # params = {'limit': 10,
# #           'after-id':1}
 
# # all_data = []
 
# # # Fetch paginated data from the API
# # while True:
# #     response = requests.get(url, params=params)
# #     if response.status_code == 200:
# #         page_data = response.json()
# #         if not page_data:  # Break if no more data
# #             break
# #         all_data.extend(page_data)
# #         params['page'] += 1  # Move to next page
# #     else:
# #         raise Exception(f"Failed to fetch data: {response.status_code}")
 
# # # Define schema
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("ibans", StringType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("address", StringType(), True)
# # ])
 
# # Create a DataFrame
# #df = spark.createDataFrame(all_data, schema)
 
# # Show the DataFrame
# #df.show()

# print("ksjfskf")


