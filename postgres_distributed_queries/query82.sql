-- start query 1 in stream 0 using template query82.tpl

SELECT i_item_id ,
       i_item_desc ,
       i_current_price
FROM item,
     inventory,
     date_dim,
     store_sales
WHERE i_current_price BETWEEN 30 AND 30+30
    AND inv_item_sk = i_item_sk
    AND d_date_sk=inv_date_sk
    AND d_date BETWEEN cast('2002-05-30' AS date) AND (cast('2002-05-30' AS date) + 60 days)
    AND i_manufact_id IN (437,
                          129,
                          727,
                          663)
    AND inv_quantity_on_hand BETWEEN 100 AND 500
    AND ss_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;

-- end query 1 in stream 0 using template query82.tpl