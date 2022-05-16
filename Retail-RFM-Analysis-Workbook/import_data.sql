CREATE TABLE public."transactions"(
transaction_date date,
transaction_hour time,
location_state varchar(100),
location_city varchar(100),
rewards_number varchar(100),
rewards_member varchar(50),
num_of_items int,
coupon_flag varchar(50),
discount_amt numeric,
order_amt varchar(50));

select * from public."transactions";
set datestyle = mdy;
COPY public."transactions" FROM 'C:\Users\yiqi1\Desktop\prep\tableau\Retail Transactions.csv' DELIMITER ','CSV Header;
ALTER TABLE transactions ALTER COLUMN order_amt TYPE numeric USING(REPLACE(order_amt,'$','')::numeric);
select * from transactions LIMIT 5;