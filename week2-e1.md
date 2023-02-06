
-- My approach was: determine the possible geo locations for the customers, determine the geo locations of suppliers, find the distances between each location and each supplier, find the minimum distance of those values for each us_city

with cust_results as (
select last_name, first_name, a.customer_city, a.customer_state, s.supplier_id, s.supplier_name, s.supplier_state, s.supplier_city,
    st_distance(cc.geo_location,sc.geo_location) as distance,
    row_number() over (partition by c.customer_id order by distance asc) as dist_result
from customers.customer_data c
join customers.customer_address a on c.customer_id = a.customer_id
join resources.us_cities cc on upper(trim(a.customer_city)) = cc.city_name and a.customer_state = cc.state_abbr
cross join suppliers.supplier_info s -- cross join with suppliers
join resources.us_cities sc on (s.supplier_state = sc.state_abbr and upper(s.supplier_city) = sc.city_name) -- join up with supplier geo data
    )

select * 
from cust_results
where dist_result = 1
order by last_name, first_name
