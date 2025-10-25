from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_to_postgres():
    try:
        logging.info("Starting Spark session...")
        spark = SparkSession.builder \
            .appName("LoadToPostgres") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.session.timeZone", "Asia/Kolkata") \
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")

        parquet_path = r"C:\Users\Asus\Contacts\Desktop\data eng\ETL_PIPELINE\data\transformed"
        
        logging.info(f"Reading data from {parquet_path}...")
        df = spark.read.parquet(parquet_path)
        logging.info("Data read successfully. Schema:")
        df.printSchema()
        logging.info("Data sample (top 5 rows):")
        df.show(5)

        # JDBC URL with TimeZone parameter
        jdbc_url = "jdbc:postgresql://localhost:5432/stock_etl?currentSchema=public&TimeZone=Asia/Kolkata"
        table_name = "stock_data"
        db_properties = {
            "user": "postgres",
            "password": "docker123",
            "driver": "org.postgresql.Driver"
        }

        logging.info(f"Writing data to PostgreSQL table: {table_name}...")
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("user", db_properties["user"]) \
          .option("password", db_properties["password"]) \
          .option("driver", db_properties["driver"]) \
          .mode("overwrite") \
          .save()

        logging.info("Data loaded successfully into PostgreSQL!")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    load_data_to_postgres()
