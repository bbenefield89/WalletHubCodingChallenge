USE wallethub_parser;

-- returns the ip_address of all that have made over 500 requests in 24hours
SELECT ip_address as OVER_500_IN_24HOURS
  FROM log_activity
WHERE created_at
      BETWEEN '2017-01-01 00:00:00'
  AND DATE_ADD('2017-01-01 00:00:00', INTERVAL 1 DAY)
GROUP BY ip_address
HAVING COUNT(*) > 500;

-- returns the ip_address of all that have made over 100 requests in 1hour
SELECT ip_address as OVER_100_IN_1HOUR
  FROM log_activity
WHERE created_at
      BETWEEN '2017-01-01 15:00:00'
  AND DATE_ADD('2017-01-01 15:00:00', INTERVAL 1 HOUR)
GROUP BY ip_address
HAVING COUNT(*) > 100;