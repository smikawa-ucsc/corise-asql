/* The query profiler helps illuminate how many rows are being operated on at each step. My main takeaway was to use where's and groupings liberally early on to reduce the result sets for later steps where possible. */


with top_recipes as (
    select 
    	parse_json(event_details):recipe_id as recipe_id,
        date(event_timestamp) as dayt,
        count(*) as view_count
    from vk_data.events.website_activity
    where parse_json(event_details):event = 'view_recipe'
    group by 1,2
    qualify row_number() over (partition by dayt order by view_count desc) = 1
),

daily_unique_sessions as (
    select
        count(distinct session_id) as session_count,
        date(event_timestamp) as dayt
    from vk_data.events.website_activity
    group by 2
    
),

session_lengths as (
	select 
        session_id, 
        date(event_timestamp) as dayt,
        min(event_timestamp) as min_time, 
        max(event_timestamp) as max_time, 
        timediff(second,min_time,max_time) as session_length
    from vk_data.events.website_activity 
    group by 1, 2
    having session_length > 0
    order by dayt
),

daily_session_lengths as (
	select 
    	avg(session_length) as average_session_length, 
        dayt
	from session_lengths
    group by dayt
),

session_search_counts as (
	select 
    	session_id, 
        date(event_timestamp) as dayt,
    	sum( case when parse_json(event_details):event = 'search' then 1 else 0 end) as session_searches, 
        sum( case when parse_json(event_details):event = 'view_recipe' then 1 else 0 end) as recipes_viewed
    from vk_data.events.website_activity
    where parse_json(event_details):event in ('search','view_recipe')
    group by 1,2
    having recipes_viewed > 0
),

daily_search_average as (
	select
    	avg(session_searches) as average_searches,
        dayt
    from session_search_counts
    group by dayt
)


select tr.dayt, recipe_id as top_recipe_id, average_session_length, average_searches, session_count
from top_recipes as tr 
	inner join daily_search_average as dsa on tr.dayt = dsa.dayt 
	inner join daily_session_lengths as dsl on tr.dayt = dsl.dayt
    inner join daily_unique_sessions as dus on tr.dayt = dus.dayt
order by dayt
