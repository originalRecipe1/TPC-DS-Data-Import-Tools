-- start query 1 in stream 0 using template query32.tpl

SELECT sum(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales ,
     item ,
     date_dim
WHERE i_manufact_id = 269
    AND i_item_sk = cs_item_sk
    AND d_date BETWEEN '1998-03-18' AND (cast('1998-03-18' AS date) + 90 days)
    AND d_date_sk = cs_sold_date_sk
    AND cs_ext_discount_amt >
        (SELECT 1.3 * avg(cs_ext_discount_amt)
         FROM catalog_sales ,
              date_dim
         WHERE cs_item_sk = i_item_sk
             AND d_date BETWEEN '1998-03-18' AND (cast('1998-03-18' AS date) + 90 days)
             AND d_date_sk = cs_sold_date_sk )
LIMIT 100;

-- end query 1 in stream 0 using template query32.tpl