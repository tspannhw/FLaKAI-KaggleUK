---- Calculate Average Watch Duration for each movie title across all users
---- https://docs.confluent.io/cloud/current/flink/reference/queries/group-aggregation.html
---- https://docs.confluent.io/cloud/current/flink/reference/queries/overview.html#flink-sql-queries
----
SELECT avg(duration) as AverageWatchDuration, `title` as MovieTitle, count(*) as NumberofViewers 
FROM `default`.`kaggleuk`.`audience`
GROUP BY `title`
