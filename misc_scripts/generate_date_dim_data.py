def generate_date_dim():
    
    # Function to generate date dimension data for a given year
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    from datetime import datetime, timedelta
    
    # create the spark session
    
    spark = None
    
    def create_date_dimension(start_date, end_date):
        current_date = start_date
        date_data = []
    
        while current_date <= end_date:
            date_dict = {
                "date": current_date,
                "day": current_date.day,
                "month": current_date.month,
                "year": current_date.year,
                "day_of_week": current_date.isoweekday(),
                "week_of_year": current_date.isocalendar()[1],
                "quarter": (current_date.month - 1) // 3 + 1,
                "is_weekend": 1 if current_date.weekday() >= 5 else 0,
                "day_name": current_date.strftime("%A"),
                "month_name": current_date.strftime("%B")
            }
            date_data.append(date_dict)
            current_date += timedelta(days=1)
    
        # Convert to DataFrame
        return spark.createDataFrame(date_data)
    
    # Set the start and end dates for the year
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Generate the date dimension DataFrame
    date_df = create_date_dimension(start_date, end_date)
    
    # Add additional columns as needed
    date_df = date_df.withColumn("day_of_year", F.date_format(F.col("date"), "D").cast(IntegerType())).withColumn("datekey", F.regexp_replace(F.split(F.col('date'), ' ').getItem(0), '-', ''))
    # Show the DataFrame (optional)
    date_df.show()
    
    date_df.write.format('parquet').partitionBy("year", "month").mode("overwrite").save(f"{output_dest_loc}date_dim/")

generate_date_dim()