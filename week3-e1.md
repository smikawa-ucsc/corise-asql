/* total unique sessions, 
	average length of sessions in seconds: - per session, max time - min time
    average number of searches completed before displaying a recipe: per session, where time < first recipe, count number of searches
    id of the recipe that was most viewed: get top count group by  recipes 
    first draft - 829ms
    	44dcd777
        1.381818
        321.4545
        178
    second - changed distinct to grouping statements
    	- 658ms

My strategy for optimization to work down the order of Snowflake evaluation, moving commands higher up the chain to lower.  The example: replacing a distinct statement with grouping to reduce load.
    */

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
    /*select 
    	count(distinct session_id) as unique_sessions
    from vk_data.events.website_activity*/
    
    
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


/*select * from vk_data.events.website_activity*/
/*
	select session_id, sum( case when parse_json(event_details):event = 'view_recipe' then 1 else 0 end)
    from vk_data.events.website_activity
    group by 1
*/
/* { "event":"view_recipe", "page":"recipe", "recipe_id":"44dcd777-5b10-41e2-90df-4dca4b696971" } */

