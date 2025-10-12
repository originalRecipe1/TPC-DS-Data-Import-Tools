-- start query 1 in stream 0 using template query3.tpl

SELECT dt.d_year ,
       item.i_brand_id brand_id ,
       item.i_brand brand ,
       sum(ss_ext_sales_price) sum_agg
FROM postgres_ds1.public.date_dim dt ,
     postgres_ds2.public.store_sales ,
     postgres_ds3.public.item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manufact_id = 436
    AND dt.d_moy=12
GROUP BY dt.d_year ,
         item.i_brand ,
         item.i_brand_id
ORDER BY dt.d_year ,
         sum_agg DESC ,
         brand_id
LIMIT 100;

-- end query 1 in stream 0 using template query3.tpl
