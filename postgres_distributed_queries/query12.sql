-- start query 1 in stream 0 using template query12.tpl

SELECT i_item_id ,
       i_item_desc ,
       i_category ,
       i_class ,
       i_current_price ,
       sum(ws_ext_sales_price) AS itemrevenue ,
       sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales ,
     item ,
     date_dim
WHERE ws_item_sk = i_item_sk
    AND i_category IN ('Jewelry',
                       'Sports',
                       'Books')
    AND ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2001-01-12' AS date) AND (cast('2001-01-12' AS date) + 30 days)
GROUP BY i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price
ORDER BY i_category ,
         i_class ,
         i_item_id ,
         i_item_desc ,
         revenueratio
LIMIT 100;

-- end query 1 in stream 0 using template query12.tpl