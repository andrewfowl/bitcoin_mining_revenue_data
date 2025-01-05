WITH grouped_transactions AS (
  SELECT
    block_timestamp,
    block_timestamp_month,
    block_number,
    is_coinbase,
    input_value,
    output_value,
    fee,
    -- Adjust grouping by the last block numbers divisible without a reminder by 2,016 (to disaggregate dificulty adjustment epochs) and 210,000 (to disaggregate the population by halving events)
    CAST(FLOOR((block_number-1) / 2016)*2016*1000000+
      FLOOR((block_number-1) / 210000)*210000 
    AS INT64) AS block_group
  FROM `bigquery-public-data.crypto_bitcoin.transactions`
)
SELECT
  block_timestamp_month,
  block_group,
  MIN(block_number) AS min_block_number,
  MAX(block_number) AS max_block_number,
  MIN(block_timestamp) AS min_block_timestamp,
  MAX(block_timestamp) AS max_block_timestamp,
  SUM(CASE WHEN is_coinbase = TRUE THEN input_value ELSE 0 END)/(100000000) AS Total_Input_Value_Coinbase,
  SUM(CASE WHEN is_coinbase = TRUE THEN output_value ELSE 0 END)/(100000000) AS Total_Output_Value_Coinbase,
  SUM(CASE WHEN is_coinbase = TRUE THEN fee ELSE 0 END)/(100000000) AS Total_Fees_Coinbase,
  SUM(CASE WHEN is_coinbase = FALSE THEN input_value ELSE 0 END)/(100000000) AS Total_Input_Value_NonCoinbase,
  SUM(CASE WHEN is_coinbase = FALSE THEN output_value ELSE 0 END)/(100000000) AS Total_Output_Value_NonCoinbase,
  SUM(CASE WHEN is_coinbase = FALSE THEN fee ELSE 0 END)/(100000000) AS Total_Fees_NonCoinbase
FROM grouped_transactions
GROUP BY block_timestamp_month, block_group
HAVING MAX(block_number)>0
ORDER BY block_timestamp_month, block_group;
