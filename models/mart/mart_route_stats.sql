
SELECT * FROM PREP_FLIGHTS;
SELECT * FROM PREP_AIRPORTS; 
WITH flight_route_stats AS (
					SELECT 
					origin
					,dest
					,count(*)
					,count(DISTINCT tail_number) AS nunique_tails
					,count(DISTINCT airline) AS num_of_airlines
					,max(arr_delay_interval) AS max_arr_delay
					,min(arr_delay_interval) AS min_arr_delay
					,sum(cancelled) AS total_canceled
					,sum(diverted) AS total_diverted 
					FROM {{ref('prep_flights')}}
					GROUP BY (dest, origin)
)
SELECT o.city AS origin_city,
		d.city AS dest_city,
		o.name AS origin_name,
		d.name AS dest_name,
		o.country AS origin_country,
		d.country AS dest_country,
		f.*
FROM flight_route_stats f
LEFT JOIN {{ref(prep_airports o)}}
	ON f.origin=o.faa
LEFT join {{ref(prep_airports d)}}
	ON f.dest=d.faa

