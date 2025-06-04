## WINDOW FUNCTION

create database new;

use new;

select * from newid;

select new_id,new_cat,Sum(new_id) Over (partition by new_cat) As "Total" from newid;


select new_id,new_cat,Sum(new_id) Over (partition by new_cat order by new_id) As "Total" from newid;

select new_id,new_cat,
sum(new_id) OVER (PARTITION BY new_cat ORDER BY new_id) AS "TOTAL",
AVG (new_id) OVER (PARTITION BY new_cat ORDER BY new_id) AS "Average" ,
count(new_id) OVER (PARTITION BY new_cat ORDER BY new_id) AS "COUNT",
max(new_id) OVER (PARTITION BY new_cat ORDER BY new_id) AS "MAX",
min(new_id) OVER (PARTITION BY new_cat ORDER BY new_id) AS "MIN"
from newid;

SELECT new_id,new_cat,
sum(new_id)over(order by new_id rows between unbounded preceding and unbounded following) 
as "Total" from newid;

select new_id, row_number() over(order by new_id)
as "Row number",
rank() over(order by new_id ) as "Rank",
dense_rank() over(order by new_id) as "Dense_rank",
percent_rank() over(order by new_id) as "Percent_rank"
from newid; 


select * from customer;
#calculate the running total of sales amount
SELECT salesid,saledate,amount,SUM(amount) OVER (ORDER BY saledate) AS "RunningTotalSales"
FROM customer;

#Rank the sales by amount for each customer.
SELECT salesid, saledate,customerid,amount,
rank() over(partition by customerid order by amount DESc)
AS "Rank BY Amount"
FROM customer;

# Calculate avg sales amount over the last 3 sales.
Select
salesid,saledate,
amount,
avg(amount) over(order by saledate Rows between 2 preceding and current row)
As "AvgLast3sales"
FROM customer;

##calculate the cumulative distribution sales amount.
SELECT
salesid,
saledate,
amount,
CUME_DIST() OVER (ORDER BY Amount) AS CumulativeDistribution
FROM customer;

##calculate the difference in sales amount 
SELECT
SaleID,
SaleDate,
Amount,
Amount - LAG(Amount,1) OVER (ORDER BY SaleDate) AS AmountDifference 
FROM Sales1;

#calculate the lead sales amount for the next sale. 
SELECT 
salesid,
saledate,
amount,
LEAD(amount,1) OVER (ORDER BY saledate) AS SALESAMOUNT
FROM customer;

SELECT 
salesid,
saledate,
amount,
Lag(amount,1) OVER (ORDER BY saledate) AS SALESAMOUNT
FROM customer;

# Find the first sale amount fir each customer.
select salesid,
saledate,
customerid,
amount,
first_value(amount) over (partition by customerid order by saledate)
as "FirstSaleAmount"
from customer;