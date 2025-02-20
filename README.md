# UPI Transactions CDC Feed

## Project Overview
This project implements a **Change Data Capture (CDC) pipeline** for UPI transactions using **Databricks, Spark Structured Streaming, and Delta Lake**. The pipeline processes real-time transaction updates, performs incremental aggregations, and maintains historical records.

## Technologies Used
- **Databricks**
- **Spark Structured Streaming**
- **Delta Lake**

## Architecture Diagram
![Architecture Diagram](path/to/architecture-diagram.png)

---

## Implementation Details

### **1. Data Sources**
- **Raw UPI Transactions** (`raw_upi_transactions_v1` Delta table)
- **Aggregated UPI Transactions** (`aggregated_upi_transactions` Delta table)

### **2. Data Ingestion & Processing**

#### **Raw Transaction Ingestion (CDC Simulation)**
1. **Create a Delta Table (`raw_upi_transactions_v1`)**:
   ```sql
   CREATE TABLE IF NOT EXISTS incremental_load.default.raw_upi_transactions_v1 (
      transaction_id STRING,
      upi_id STRING,
      merchant_id STRING,
      transaction_amount DOUBLE,
      transaction_timestamp TIMESTAMP,
      transaction_status STRING
   ) USING DELTA
   TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
   ```

2. **Insert and Update Transactions in Batches**:
   - **Batch 1:** New transactions are inserted.
   - **Batch 2:** Transactions are updated (e.g., status change from `initiated` to `completed`).
   - **Batch 3:** Refund transactions are processed.

3. **Merge New Data into the Delta Table**:
   ```python
   def merge_to_delta_table(delta_table_name, batch_df):
       delta_table = DeltaTable.forName(spark, delta_table_name) 
       delta_table.alias('target')\
           .merge(batch_df.alias('source'), "target.transaction_id = source.transaction_id")\
           .whenMatchedUpdate(set={
               "upi_id": "source.upi_id",
               "merchant_id": "source.merchant_id",
               "transaction_amount": "source.transaction_amount",
               "transaction_timestamp": "source.transaction_timestamp",
               "transaction_status": "source.transaction_status"
           })\
           .whenNotMatchedInsertAll()\
           .execute()
   ```

#### **Real-time Merchant Aggregation (`realtime_merchant_aggregation.ipynb`)**
1. **Create an Aggregated Transactions Table**:
   ```sql
   CREATE TABLE IF NOT EXISTS incremental_load.default.aggregated_upi_transactions (
       merchant_id STRING,
       total_sales DOUBLE,
       total_refunds DOUBLE,
       net_sales DOUBLE
   ) USING DELTA;
   ```
2. **Stream Changes from CDC Feed**:
   - Read changes using **Change Data Feed (CDF)** from `raw_upi_transactions_v1`.
   - Only process **inserts and updates** (`_change_type` = `insert`, `update_postimage`).

3. **Process and Aggregate Data in Micro-batches**:
   ```python
   def process_aggregation(batch_df, batch_id):
       aggregated_df = (
           batch_df 
           .filter(col("_change_type").isin("insert", "update_postimage"))
           .groupBy("merchant_id")
           .agg(
               sum(when(col("transaction_status") == "completed", col("transaction_amount")).otherwise(0)).alias("total_sales"),
               sum(when(col("transaction_status") == "refunded", col("transaction_amount")).otherwise(0)).alias("total_refunds")
           )
           .withColumn("net_sales", col("total_sales") - col("total_refunds"))
       )
       
       target_table = DeltaTable.forName(spark, "incremental_load.default.aggregated_upi_transactions")
       target_table.alias("target")\
           .merge(aggregated_df.alias("source"), "target.merchant_id = source.merchant_id")\
           .whenMatchedUpdate(set={
               "total_sales": "target.total_sales + source.total_sales",
               "total_refunds": "target.total_refunds + source.total_refunds",
               "net_sales": "target.net_sales + source.net_sales"
           })\
           .whenNotMatchedInsertAll()\
           .execute()
   ```

4. **Start Streaming Aggregation**:
   ```python
   cdc_stream = spark.readStream.format("delta").option("readChangeFeed", "true").table("raw_upi_transactions_v1")
   cdc_stream.writeStream.foreachBatch(process_aggregation).outputMode("update").start().awaitTermination()
   ```

---

## **How to Run the Project**
1. Clone the repository and configure Databricks.
2. Create required Delta tables (`raw_upi_transactions_v1` and `aggregated_upi_transactions`).
3. Insert sample transaction data into `raw_upi_transactions_v1`.
4. Run the CDC processing script to merge transactions.
5. Start the streaming job to compute merchant aggregations.
6. Validate the data in the aggregated table.

---

## **Project Folder Structure**
```
├── notebooks/            # Databricks Notebooks
│   ├── upi_merchant_pay_trx_mock_data.ipynb
│   ├── realtime_merchant_aggregation.ipynb
│
├── data/                 # Sample datasets
│
├── docs/                 # Documentation & workflow files
│   ├── project-explanation.txt
│
├── README.md             # Main documentation
```

---

## **Future Enhancements**
- Implement **real-time fraud detection** by tracking suspicious transactions.
- Improve **scalability** with partitioned tables.
- Add **automated testing** using PyTest or Great Expectations.
- Integrate with **Power BI for visualization**.

---

## **Contact**
For queries or contributions, contact [Your Name] at [Your Email].
