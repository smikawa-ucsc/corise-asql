
with customer_food_preferences as ( /* pulled this out as a cte */
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),

chicago_il_geolocation as ( /* pulled this out as a cte */
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),

gary_in_geolocation as ( /* pulled this out as a cte */
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)

select 
    first_name || ' ' || last_name as customer_name,
    ca.customer_city,
    ca.customer_state,
    c_prefs.food_pref_count,
    (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from 
    vk_data.customers.customer_address as ca
    inner join vk_data.customers.customer_data as customer on ca.customer_id = customer.customer_id
    left join vk_data.resources.us_cities as us 
        on upper(trim(ca.customer_state)) = upper(trim(us.state_abbr)) /* lowered case of functions, replaced left/right trim with trim */
        and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
    inner join customer_food_preferences as c_prefs 
        on customer.customer_id = c_prefs.customer_id /* added inner, renamed s to c_prefs */
    cross join chicago_il_geolocation as chic
    cross join gary_in_geolocation as gary
where   
    (customer_state = 'KY' and (trim(city_name) ilike any('%concord%', '%georgetown%', '%ashland%'))) /*ilike any to reduce comparisons */
    or (customer_state = 'CA' and (trim(city_name) ilike any('%oakland%', '%pleasant hill%'))) /*ilike any to reduce comparisons */
    or (customer_state = 'TX' and (trim(city_name) ilike '%arlington%') 
    or trim(city_name) ilike '%brownsville%') /* Formatting was confusing here. brownsville is not treated like other cities and restricted to TX, it seems to be an overall OR condition as written.  It's formatted without a newline as if it's a city+state comparison like the others.Ambiguous wether intentional or an accident */
