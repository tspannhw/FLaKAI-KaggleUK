
---- Calculate the average watch duration for each movie title across all users.

SELECT avg(duration) as AverageWatchDuration, `title` as MovieTitle, count(*) as NumberofViewers 
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`

---- Analyze daily engagement patterns for each movie title. Calculate daily view counts and total watch time for each title to track how user interest fluctuates day by day.
SELECT `title` as MovieTitle, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') as ViewDate, count(*) as DailyViewCount, 
  sum(duration) as TotalWatchTimeSeconds, 
  (sum(duration)/60) as TotalWatchTimeMinutes, 
  (sum(duration)/3600) as TotalWatchTimeHours
FROM `default`.`kaggleuk`.`netflixbehavior`
GROUP BY `title`, DATE_FORMAT(`datetime`, 'yyyy-MM-dd') 
