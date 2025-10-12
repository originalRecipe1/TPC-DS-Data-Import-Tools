-- start query 1 in stream 0 using template query16.tpl

SELECT count(DISTINCT cs_order_number) AS "order count" ,
       sum(cs_ext_ship_cost) AS "total shipping cost" ,
       sum(cs_net_profit) AS "total net profit"
FROM postgres_ds3.public.catalog_sales cs1 ,
     postgres_ds2.public.date_dim ,
     postgres_ds1.public.customer_address ,
     postgres_ds1.public.call_center
WHERE d_date BETWEEN '1999-2-01' AND (cast('1999-2-01' AS date) + 60 days)
    AND cs1.cs_ship_date_sk = d_date_sk
    AND cs1.cs_ship_addr_sk = ca_address_sk
    AND ca_state = 'IL'
    AND cs1.cs_call_center_sk = cc_call_center_sk
    AND cc_county IN ('Williamson County',
                      'Williamson County',
                      'Williamson County',
                      'Williamson County',
                      'Williamson County')
    AND EXISTS
        (SELECT *
         FROM postgres_ds1.public.catalog_sales cs2
         WHERE cs1.cs_order_number = cs2.cs_order_number
             AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
    AND NOT exists
        (SELECT *
         FROM postgres_ds3.public.catalog_returns cr1
         WHERE cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;

-- end query 1 in stream 0 using template query16.tpl
