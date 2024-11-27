
---- Calculate the average watch duration for each movie title across all users.
---- https://docs.confluent.io/cloud/current/flink/reference/queries/group-aggregation.html
---- https://docs.confluent.io/cloud/current/flink/reference/queries/overview.html#flink-sql-queries
----
SELECT avg(duration) as AverageWatchDuration, `title` as MovieTitle, count(`user_id`) as NumberofViewers 
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`

---- Analyze daily engagement patterns for each movie title. Calculate daily view counts and total watch time for each title to track how user interest fluctuates day by day.

SELECT `title` as MovieTitle, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate, count(`user_id`) as DailyViewCount, 
  sum(duration) as TotalWatchTimeSeconds, 
  (sum(duration)/60) as TotalWatchTimeMinutes, 
  (sum(duration)/3600) as TotalWatchTimeHours
FROM TABLE(
  TUMBLE(TABLE  `default`.`kaggleuk`.`netflixbehavior`, 
  DESCRIPTOR( $rowtime ), 
  INTERVAL '1' DAY))
GROUP BY `title`, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') 

---- 
  
CREATE TABLE behaviorupdate as (
SELECT `title` as MovieTitle, avg(duration) as AverageWatchDuration,  count(`user_id`) as NumberofViewers 
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`
)

show create table `behaviorupdate`

describe extended  `default`.`kaggleuk`.`netflixbehavior`

SELECT `title` as MovieTitle, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate, count(`user_id`) as DailyViewCount, 
  sum(duration) as TotalWatchTimeSeconds, 
  (sum(duration)/60) as TotalWatchTimeMinutes, 
  (sum(duration)/3600) as TotalWatchTimeHours
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') 
