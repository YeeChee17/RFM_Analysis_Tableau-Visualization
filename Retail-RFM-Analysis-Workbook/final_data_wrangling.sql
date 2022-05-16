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