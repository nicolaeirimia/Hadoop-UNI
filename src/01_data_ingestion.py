from pyspark.sql import SparkSession
import os
import time

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Yelp Data Ingestion - Time Tracking") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_dir = os.path.join(base_dir, "data", "raw")
    processed_dir = os.path.join(base_dir, "data", "processed")

    def process_file(filename, output_name, format_type):
        print(f"\n--- Processing {filename} ---")
        file_path = os.path.join(raw_dir, filename)
        
        if os.path.exists(file_path):
            start_time = time.time()
            df = spark.read.json(file_path)
            output_path = os.path.join(processed_dir, output_name)
            
            if format_type == "parquet":
                df.write.mode("overwrite").parquet(output_path)
            elif format_type == "orc":
                df.write.mode("overwrite").orc(output_path)
                
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"SUCCESS: Saved {output_name} as {format_type.upper()}.")
            print(f"TIME TAKEN: {elapsed_time:.2f} seconds.")
        else:
            print(f"ERROR: File not found -> {file_path}")

    # Track total pipeline time
    pipeline_start = time.time()

    # Process all 5 datasets
    process_file("yelp_academic_dataset_business.json", "business.parquet", "parquet")
    process_file("yelp_academic_dataset_review.json", "review.orc", "orc")
    process_file("yelp_academic_dataset_user.json", "user.orc", "orc")
    process_file("yelp_academic_dataset_checkin.json", "checkin.parquet", "parquet")
    process_file("yelp_academic_dataset_tip.json", "tip.parquet", "parquet")

    pipeline_end = time.time()
    print(f"\n=====================================")
    print(f"TOTAL PIPELINE EXECUTION TIME: {(pipeline_end - pipeline_start) / 60:.2f} minutes.")
    print(f"=====================================")
    
    spark.stop()

if __name__ == "__main__":
    main()