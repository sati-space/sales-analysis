import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, when, to_date, sum as spark_sum, avg, year, first, lit, expr, round


def initialize_spark():
    return SparkSession.builder.appName("SalesAnalysis").getOrCreate()


def load_data(spark):
    return {
        'raw_sales_order_detail': spark.read.csv(
            "sales-order-detail-1-.csv",
            header=True,
            inferSchema=True
        )
        .withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
        .withColumn("SalesOrderDetailID", col("SalesOrderDetailID").cast(IntegerType()))
        .withColumn("ProductID", col("ProductID").cast(IntegerType()))
        .withColumn("OrderQty", col("OrderQty").cast(IntegerType()))
        .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
        .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast(DoubleType())),

        'raw_sales_order_header': spark.read.csv(
            "sales-order-header-1-.csv",
            header=True,
            inferSchema=True
        )
        .withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
        .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))
        .withColumn("ShipDate", to_date(col("ShipDate"), "yyyy-MM-dd"))
        .withColumn("Freight", col("Freight").cast(DoubleType())),

        'raw_products': spark.read.csv(
            "products-1-.csv",
            header=True,
            inferSchema=True
        )
        .withColumn("ProductID", col("ProductID").cast(IntegerType()))
        .withColumn("Color", col("Color").cast(StringType()))
        .withColumn("ProductCategoryName", col("ProductCategoryName").cast(StringType()))
        .withColumn("ProductSubCategoryName", col("ProductSubCategoryName").cast(StringType()))
    }


def transform_product_master(product_master):
    return (
        product_master
        .withColumn(
            "Color",
            when(
                col("Color").isNull(), lit("N/A")).otherwise(col("Color"))
        )
        .withColumn(
            "ProductCategoryName",
            when(
                col("ProductCategoryName").isNull() &
                col("ProductSubCategoryName").isin(
                    "Gloves", "Shorts", "Socks", "Tights", "Vests"
                ),
                "Clothing"
            )
            .when(
                col("ProductCategoryName").isNull() &
                col("ProductSubCategoryName").isin(
                    "Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"
                ),
                "Accessories"
            )
            .when(
                col("ProductCategoryName").isNull() & (
                        col("ProductSubCategoryName").contains("Frames") |
                        col("ProductSubCategoryName").isin("Wheels", "Saddles")
                ),
                "Components"
            )
            .otherwise(col("ProductCategoryName"))
        )
    )


def transform_sales_data(sales_order_detail, sales_order_header, product_master):
    publish_sales = (
        sales_order_detail
        .join(sales_order_header, "SalesOrderID", "inner")
    )

    publish_sales = publish_sales.withColumn(
        "LeadTimeInBusinessDays",
        expr(
            """
                size(filter(sequence(OrderDate, ShipDate, interval 1 day), 
                x -> weekday(x) NOT IN (6,7))) - 1
            """
        )
    )

    publish_sales = publish_sales.withColumn(
        "TotalLineExtendedPrice",
        col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount"))
    )

    publish_sales = publish_sales.withColumnRenamed("Freight", "TotalOrderFreight")

    publish_sales = publish_sales.withColumn("Year", year(col("OrderDate")))

    publish_sales = publish_sales.join(product_master, "ProductID", "left")
    return publish_sales


def save_data_as_csv(df, file_name):
    folder = "store/" if file_name.startswith("store_") else \
        "publish/" if file_name.startswith("publish_") else "raw/"

    if not os.path.exists(folder):
        os.makedirs(folder)

    temp_path = folder + file_name + "_temp"
    df.coalesce(1).write.csv(temp_path, header=True, mode="overwrite")

    for file in os.listdir(temp_path):
        if file.startswith("part-") and file.endswith(".csv"):
            shutil.move(os.path.join(temp_path, file), folder + file_name + ".csv")

    shutil.rmtree(temp_path)


def analyze_data(publish_sales):
    revenue_by_color = (
        publish_sales.groupBy("Year", "Color")
        .agg(spark_sum("TotalLineExtendedPrice").alias("TotalRevenue"))
    )

    most_revenue_color = (
        revenue_by_color
        .orderBy(col("Year"), col("TotalRevenue").desc())
        .groupBy("Year")
        .agg(
            first("Color").alias("TopColor"),
            first("TotalRevenue").cast("decimal(12,2)").alias("TotalRevenue")
        )
    )

    lead_time_avg = (
        publish_sales.filter(col("ProductCategoryName").isNotNull())
        .groupBy("ProductCategoryName")
        .agg(round(avg("LeadTimeInBusinessDays"), 2).alias("AvgLeadTime (Days)"))
    )

    most_revenue_color.show()
    lead_time_avg.show()


def main():
    spark = initialize_spark()
    try:
        data = load_data(spark)

        save_data_as_csv(data['raw_products'], "raw_products")
        save_data_as_csv(data['raw_sales_order_detail'], "raw_sales_order_detail")
        save_data_as_csv(data['raw_sales_order_header'], "raw_sales_order_header")

        store_sales_order_detail = data['raw_sales_order_detail']
        store_sales_order_header = data['raw_sales_order_header']

        save_data_as_csv(store_sales_order_detail, "store_sales_order_detail")
        save_data_as_csv(store_sales_order_header, "store_sales_order_header")

        publish_product = transform_product_master(data['raw_products'])
        save_data_as_csv(publish_product, "publish_product")

        publish_sales = transform_sales_data(
            store_sales_order_detail,
            store_sales_order_header,
            publish_product
        )
        save_data_as_csv(publish_sales, "publish_sales")

        analyze_data(publish_sales)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
