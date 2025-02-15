-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata
-- MAGIC

-- COMMAND ----------

DESCRIBE History employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'
