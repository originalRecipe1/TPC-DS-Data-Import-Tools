-- start query 1 in stream 0 using template query7.tpl

SELECT i_item_id,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
FROM postgres_ds2.public.store_sales,
     postgres_ds3.public.customer_demographics,
     postgres_ds1.public.date_dim,
     postgres_ds2.public.item,
     postgres_ds3.public.promotion
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_promo_sk = p_promo_sk
    AND cd_gender = 'F'
    AND cd_marital_status = 'W'
    AND cd_education_status = 'Primary'
    AND (p_channel_email = 'N'
         OR p_channel_event = 'N')
    AND d_year = 1998
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;

-- end query 1 in stream 0 using template query7.tpl
