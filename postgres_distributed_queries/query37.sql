-- start query 1 in stream 0 using template query37.tpl

SELECT i_item_id ,
       i_item_desc ,
       i_current_price
FROM item,
     inventory,
     date_dim,
     catalog_sales
WHERE i_current_price BETWEEN 22 AND 22 + 30
    AND inv_item_sk = i_item_sk
    AND d_date_sk=inv_date_sk
    AND d_date BETWEEN cast('2001-06-02' AS date) AND (cast('2001-06-02' AS date) + 60 days)
    AND i_manufact_id IN (678,
                          964,
                          918,
                          849)
    AND inv_quantity_on_hand BETWEEN 100 AND 500
    AND cs_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;

-- end query 1 in stream 0 using template query37.tpl