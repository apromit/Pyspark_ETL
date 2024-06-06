--create database prc;
use prc;

create table test_orders(
		[order_id] int primary key,
		[order_date]date,     
		[ship_mode] varchar(20),           
		[segment] varchar(20),             
		[country] varchar(20),  
		[city] varchar(20),
		[state] varchar(20),
		[postal_code] varchar(20),
		[region] varchar(20),
		[category] varchar(20),
		[sub_category] varchar(20),
		[product_id] varchar(50),
		[quantity]   int,    
		[discount_given] decimal(7,2),
		[sale_price]   decimal(7,2),
		[profit] decimal(7,2),
		[file_date] date

);

--finding top 10 highest selling product

select top 10 [product_id],sum([sale_price]) as total_sale from test_orders
group by [product_id] order by total_sale desc;


--top 5 highest selling product in each region


with temp as (
select [region],
[product_id],
sum([sale_price]) as total_sales
from test_orders
group by [region],[product_id])

 select temp2.region,temp2.product_id,temp2.total_sales from 
(select *,
ROW_NUMBER() over(partition by region order by total_sales desc) as rn
from temp) temp2
where rn<=5;



--find month over month comparison for 2022 and 2023 sales, eg- Jan2022 vs Jan2023

with temp3 as (select YEAR(order_date) as Order_Year,
month(order_date) as Order_Month,
sum(sale_price) as total_sales_monthly from test_orders
group by YEAR(order_date),month(order_date))

select Order_month,
sum(case when Order_Year = 2022 then total_sales_monthly else 0 end) as order_2022,
sum(case when Order_Year = 2023 then total_sales_monthly else 0 end) as order_2023
from temp3
group by Order_Month
order by Order_Month


--for each category which month had highest sale

select * from test_orders

with temp4 as( 
select
category,
CONCAT(DATENAME(MONTH,order_date),'-',FORMAT(order_date,'yyyy')) as orderyearmonth,
sum(sale_price) as total_sales
from test_orders
group by category,CONCAT(DATENAME(MONTH,order_date),'-',FORMAT(order_date,'yyyy')))
select Z.category,Z.orderyearmonth,Z.total_sales from (select *,
ROW_NUMBER() over(partition by category order by total_sales desc) as rn
from temp4) Z
where Z.rn=1;