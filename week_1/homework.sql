-- Question 1
Select count(*) from yellow_taxi_trips_csv
where DATE_TRUNC('DAY', lpep_pickup_datetime) = '2019-01-15 00:00:00'
and DATE_TRUNC('DAY', lpep_dropoff_datetime) = '2019-01-15 00:00:00'

-- Questiion 2
Select lpep_pickup_datetime from yellow_taxi_trips_csv
where trip_distance = (Select max(trip_distance) from yellow_taxi_trips_csv)

--Question 3 
Select sum(case when passenger_count=2 then 1 else 0 end) as "Two",
	   sum(case	when passenger_count=3 then 1 else 0 end) as "Three"
from yellow_taxi_trips_csv
where DATE_TRUNC('DAY', lpep_pickup_datetime) = '2019-01-01 00:00:00'

--Question 4
SELECT max(tip_amount),pi."Zone"
FROM yellow_taxi_trips_csv tr
join taxi_lookup l on tr."PULocationID"=l."LocationID"
join taxi_lookup pi on tr."DOLocationID" = pi."LocationID"
where l."Zone" = 'Astoria'
group by pi."Zone"
order by 1 desc
limit 1;
