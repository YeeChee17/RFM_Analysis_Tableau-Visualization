--RFM analysis
--Pretend today is 2020-10-31
SELECT MIN(transaction_date), MAX(transaction_date)
FROM transactions;
-- 2019-10-01 : 2020-10-30

--Step1: filter out all reward_member
SELECT *
FROM transactions
WHERE rewards_number IS NOT NULL
AND rewards_member = 'true';

--Step2: find the average amt over all transactions 127.84
-- find average amt, frequency, latest_date by each member
SELECT *,
	ROUND(AVG(order_amt)OVER(),2) AS avg_amt_all,
	ROUND(AVG(order_amt)OVER(Partition By rewards_number),2) AS avg_amt,
	COUNT(rewards_number)OVER(Partition By rewards_number) AS frequency,
	MAX(transaction_date)OVER(Partition By rewards_number) AS latest_date
FROM transactions
WHERE rewards_number IS NOT NULL
AND rewards_member = 'true';
--  find the time difference between latest_date and fake today
CREATE MATERIALIZED VIEW members_stats AS
SELECT *,
	today_date - latest_date AS recent_days,
	ROUND(AVG(frequency)OVER(),4) AS avg_freq,
	ROUND(AVG(today_date - latest_date)OVER(),2) AS avg_reday
FROM (SELECT *,
			ROUND(AVG(order_amt)OVER(),2) AS avg_amt_all,
			ROUND(AVG(order_amt)OVER(Partition By rewards_number),2) AS avg_amt,
			COUNT(rewards_number)OVER(Partition By rewards_number) AS frequency,
			MAX(transaction_date)OVER(Partition By rewards_number) AS latest_date,
	  		'2020-10-31'::date AS today_date
		FROM transactions
		WHERE rewards_number IS NOT NULL
		AND rewards_member = 'true') AS temp_t;

SELECT*,
	CONCAT("R","F","M") AS "RFM"
FROM(SELECT*,
		CASE WHEN recent_days <= 60 THEN '1' ELSE '0' END AS "R",
		CASE WHEN frequency > avg_freq THEN '1' ELSE '0' END AS "F",
		CASE WHEN avg_amt > avg_amt_all THEN '1' ELSE '0' END AS "M"
	FROM members_stats)AS temp_t;
	
CREATE TABLE rfmLabeled AS
SELECT*,
	CONCAT("R","F","M") AS "RFM"
FROM(SELECT*,
		CASE WHEN recent_days <= 60 THEN '1' ELSE '0' END AS "R",
		CASE WHEN frequency > avg_freq THEN '1' ELSE '0' END AS "F",
		CASE WHEN avg_amt > avg_amt_all THEN '1' ELSE '0' END AS "M"
	FROM members_stats)AS temp_t;

COPY rfmLabeled 
TO 'C:\Users\yiqi1\Desktop\prep\tableau\labeled_retail.csv'
WITH DELIMITER ','
CSV
HEADER;

SELECT *
FROM information_schema.tables;

SELECT *
FROM members_stats
WHERE recent_days < 60

WHERE rewards_number = '202-501-13'
SELECT *
FROM transactions
WHERE rewards_number = '202-501-13';
SELECT *
FROM transactions
WHERE rewards_member = 'false';
--group by的问题就是keep不了其他的column
--ROUND(AVG(order_amt)OVER(),2) AS avg_amt_all,也用不了
SELECT rewards_number, COUNT(rewards_number)
FROM transactions
GROUP BY rewards_number
ORDER BY 2 DESC