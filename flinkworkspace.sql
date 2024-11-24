---- Calculate Average Watch Duration for each movie title across all users
---- https://docs.confluent.io/cloud/current/flink/reference/queries/group-aggregation.html
---- https://docs.confluent.io/cloud/current/flink/reference/queries/overview.html#flink-sql-queries
---- https://www.kaggle.com/datasets/vodclickstream/netflix-audience-behaviour-uk-movies
SELECT avg(duration) as AverageWatchDuration, `title` as MovieTitle, count(*) as NumberofViewers 
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`

describe `default`.`kaggleuk`.`netflixbehavior`

SELECT TO_TIMESTAMP(`datetime`, 'yyyy-MM-dd hh:mm:ss')
FROM `default`.`kaggleuk`.`netflixbehavior`

---Analyze daily engagement patterns for each movie title. Calculate daily view counts and 
---  total watch time for each title to track how user interest fluctuates day by day.


SELECT `title` as MovieTitle, sum(duration) as TotalWatchTime, count(*) as DailyWatchCount,
      DAYOFYEAR(TO_TIMESTAMP(`datetime`, 'yyyy-MM-dd hh:mm:ss')) as DayView
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`, DAYOFYEAR(TO_TIMESTAMP(`datetime`, 'yyyy-mm-dd hh:mm:ss'))


SELECT DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate
FROM `default`.`kaggleuk`.`netflixbehavior`

SELECT `title` as MovieTitle, sum(duration) as TotalWatchTime, count(*) as DailyWatchCount,
       DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') 

---- yyyy-MM-dd
---- https://docs.oracle.com/en/java/javase/19/docs/api/java.base/java/text/SimpleDateFormat.html

SELECT `title` as MovieTitle, sum(duration) as TotalWatchTime, (sum(duration)/60) as TotalWatchTimeMin, 
  (sum(duration)/3600) as TotalWatchTimeH, 
       count(*) as DailyWatchCount,
       DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') 

