with top_recipe as (
    select 
    	parse_json(event_details):recipe_id as recipe_id, 
        count(*) as recipe_view
    from vk_data.events.website_activity
    where parse_json(event_details):event = 'view_recipe'
    group by 1
    order by 2 desc
    limit 1
),

total_unique_sessions as (
    select count(*) as unique_sessions
    from (
        select count(session_id)
        from vk_data.events.website_activity
        group by session_id)
    
),

session_lengths as (
	select 
    	session_id, 
        min(event_timestamp) as min_time, 
        max(event_timestamp) as max_time, 
        timediff(second,min_time,max_time) as session_length
    from vk_data.events.website_activity
    group by 1
    having session_length > 0
),

session_length_average as (
    select avg(session_length) as session_length
    from session_lengths
),

session_search_counts as (
	select 
    	session_id, 
    	sum( case when parse_json(event_details):event = 'search' then 1 else 0 end) as session_searches, 
        sum( case when parse_json(event_details):event = 'view_recipe' then 1 else 0 end) as recipes_viewed
    from vk_data.events.website_activity
    group by session_id
    having recipes_viewed > 0
),

session_search_average as (
	select
    	avg(session_searches) as average
    from session_search_counts
)


select
(select recipe_id from top_recipe) as top_recipe_guid,
(select average from session_search_average) as session_search_average,
(select session_length from session_length_average) as average_session_length_seconds,
(select unique_sessions from total_unique_sessions) as total_unique_sessions
