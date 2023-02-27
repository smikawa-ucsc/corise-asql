/* 
	Apply automobile customer and urgent filters to all queries.
    Limit 100 and order_date desc in final result

*/

/* determine each customer's last order date */
with customer_last_order as (
	select 
    	c_custkey, 
    	max(o_orderdate) as last_order_date
    from customer 
    	inner join orders on c_custkey = o_custkey
    where c_mktsegment = 'AUTOMOBILE' 
    	and o_orderpriority = '1-URGENT'
    group by 1
),

/* determine the 3 highest dollar orders */
orders_ranked as (
	select 
    	c_custkey,
        o_orderkey,
        o_totalprice
    from customer 
    	inner join orders on c_custkey = o_custkey
    where c_mktsegment = 'AUTOMOBILE' 
    	and o_orderpriority = '1-URGENT'
    qualify row_number() over (partition by c_custkey order by o_totalprice desc) <= 3  
),

/* aggregate the 3 highest dollar orders; total spent and order keys */
orders_agg as (
    select 
    	c_custkey,
     	listagg(o_orderkey,',') within group (order by o_totalprice desc) as highest_orderkeys,
        sum(o_totalprice) as total_spent
    from orders_ranked
    group by 1
),

/* determine the 3 highest dollar spent parts */
partslist_ranked as (
    select 
        c_custkey,
        l_partkey,
        l_extendedprice,
        l_quantity
    from customer 
    	inner join orders on c_custkey = o_custkey
        inner join lineitem on o_orderkey = l_orderkey
    where c_mktsegment = 'AUTOMOBILE' 
        and o_orderpriority = '1-URGENT'
    qualify row_number() over (partition by c_custkey order by l_extendedprice desc) <= 3  
),

/* aggregate the 3 highest dollar spent parts; keys, price, quantity */
partslist_agg as (
	select 
    	c_custkey,
        listagg(l_partkey,',') within group (order by l_extendedprice desc) as partskeys,
        listagg(l_extendedprice,',') within group (order by l_extendedprice desc) as partsprice,
        listagg(l_quantity,',') within group (order by l_extendedprice desc) as partsqty
    from partslist_ranked
    group by 1
)

/* build the final query */
select 
	c_custkey, 
    last_order_date, 
    highest_orderkeys as order_numbers, 
    total_spent,
    replace(get(split(partskeys,','),0),'"','') as part_1_key,
    replace(get(split(partsqty,','),0),'"','') as part_1_quantity,
    replace(get(split(partsprice,','),0),'"','') as part_1_total_spent,
    replace(get(split(partskeys,','),1),'"','') as part_2_key,
    replace(get(split(partsqty,','),1),'"','') as part_2_quantity,
    replace(get(split(partsprice,','),1),'"','') as part_2_total_spent,
    replace(get(split(partskeys,','),2),'"','') as part_3_key,
    replace(get(split(partsqty,','),2),'"','') as part_3_quantity,
    replace(get(split(partsprice,','),2),'"','') as part_3_total_spent
from customer_last_order
	natural join orders_agg 
    natural join partslist_agg
where c_custkey = '68000'
order by last_order_date desc
limit 100

/*
Review of candidate submission:
1) Do you agree with the results returned by the query? 
	I do not.  The last order date was restricted by the most-spend filter, so it's not truly the last order date among urgent orders.
2) Is it easy to understand?
	Relatively. I would say yes, the error is logical and not necessarily obfuscated by naming or formatting.
3) Could the code be more efficient?
	The parts join could be removed.  The parts key can be just used from the lineitem.
*/
