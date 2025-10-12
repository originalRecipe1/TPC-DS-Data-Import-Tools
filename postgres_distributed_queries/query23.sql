-- start query 1 in stream 0 using template query23.tpl
WITH frequent_ss_items AS
    (SELECT substr(i_item_desc, 1, 30) itemdesc,
            i_item_sk item_sk,
            d_date solddate,
            count(*) cnt
     FROM postgres_ds2.store_sales ,
          postgres_ds3.date_dim ,
          postgres_ds1.item
     WHERE ss_sold_date_sk = d_date_sk
         AND ss_item_sk = i_item_sk
         AND d_year IN (1999,
                        1999+1,
                        1999+2,
                        1999+3)
     GROUP BY substr(i_item_desc, 1, 30),
              i_item_sk,
              d_date
     HAVING count(*) >4),
     max_store_sales AS
    (SELECT max(csales) tpcds_cmax
     FROM
         (SELECT c_customer_sk,
                 sum(ss_quantity*ss_sales_price) csales
          FROM postgres_ds2.store_sales ,
               postgres_ds3.customer ,
               postgres_ds1.date_dim
          WHERE ss_customer_sk = c_customer_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year IN (1999,
                             1999+1,
                             1999+2,
                             1999+3)
          GROUP BY c_customer_sk)),
     best_ss_customer AS
    (SELECT c_customer_sk,
            sum(ss_quantity*ss_sales_price) ssales
     FROM postgres_ds1.store_sales ,
          postgres_ds2.customer
     WHERE ss_customer_sk = c_customer_sk
     GROUP BY c_customer_sk
     HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) *
         (SELECT *
          FROM max_store_sales))
SELECT sum(sales)
FROM
    (SELECT cs_quantity*cs_list_price sales
     FROM postgres_ds1.catalog_sales ,
          postgres_ds3.date_dim
     WHERE d_year = 1999
         AND d_moy = 1
         AND cs_sold_date_sk = d_date_sk
         AND cs_item_sk IN
             (SELECT item_sk
              FROM frequent_ss_items)
         AND cs_bill_customer_sk IN
             (SELECT c_customer_sk
              FROM best_ss_customer)
     UNION ALL SELECT ws_quantity*ws_list_price sales
     FROM postgres_ds2.web_sales ,
          postgres_ds3.date_dim
     WHERE d_year = 1999
         AND d_moy = 1
         AND ws_sold_date_sk = d_date_sk
         AND ws_item_sk IN
             (SELECT item_sk
              FROM frequent_ss_items)
         AND ws_bill_customer_sk IN
             (SELECT c_customer_sk
              FROM best_ss_customer))
LIMIT 100;

WITH frequent_ss_items AS
    (SELECT substr(i_item_desc, 1, 30) itemdesc,
            i_item_sk item_sk,
            d_date solddate,
            count(*) cnt
     FROM postgres_ds3.store_sales ,
          postgres_ds2.date_dim ,
          postgres_ds1.item
     WHERE ss_sold_date_sk = d_date_sk
         AND ss_item_sk = i_item_sk
         AND d_year IN (1999,
                        1999 + 1,
                        1999 + 2,
                        1999 + 3)
     GROUP BY substr(i_item_desc, 1, 30),
              i_item_sk,
              d_date
     HAVING count(*) >4),
     max_store_sales AS
    (SELECT max(csales) tpcds_cmax
     FROM
         (SELECT c_customer_sk,
                 sum(ss_quantity*ss_sales_price) csales
          FROM postgres_ds2.store_sales ,
               postgres_ds3.customer ,
               postgres_ds1.date_dim
          WHERE ss_customer_sk = c_customer_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year IN (1999,
                             1999+1,
                             1999+2,
                             1999+3)
          GROUP BY c_customer_sk)),
     best_ss_customer AS
    (SELECT c_customer_sk,
            sum(ss_quantity*ss_sales_price) ssales
     FROM postgres_ds2.store_sales ,
          postgres_ds1.customer
     WHERE ss_customer_sk = c_customer_sk
     GROUP BY c_customer_sk
     HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) *
         (SELECT *
          FROM max_store_sales))
SELECT c_last_name,
       c_first_name,
       sales
FROM
    (SELECT c_last_name,
            c_first_name,
            sum(cs_quantity*cs_list_price) sales
     FROM postgres_ds3.catalog_sales ,
          postgres_ds1.customer ,
          postgres_ds2.date_dim
     WHERE d_year = 1999
         AND d_moy = 1
         AND cs_sold_date_sk = d_date_sk
         AND cs_item_sk IN
             (SELECT item_sk
              FROM frequent_ss_items)
         AND cs_bill_customer_sk IN
             (SELECT c_customer_sk
              FROM best_ss_customer)
         AND cs_bill_customer_sk = c_customer_sk
     GROUP BY c_last_name,
              c_first_name
     UNION ALL SELECT c_last_name,
                      c_first_name,
                      sum(ws_quantity*ws_list_price) sales
     FROM postgres_ds3.web_sales ,
          postgres_ds2.customer ,
          postgres_ds1.date_dim
     WHERE d_year = 1999
         AND d_moy = 1
         AND ws_sold_date_sk = d_date_sk
         AND ws_item_sk IN
             (SELECT item_sk
              FROM frequent_ss_items)
         AND ws_bill_customer_sk IN
             (SELECT c_customer_sk
              FROM best_ss_customer)
         AND ws_bill_customer_sk = c_customer_sk
     GROUP BY c_last_name,
              c_first_name)
ORDER BY c_last_name,
         c_first_name,
         sales
LIMIT 100;

-- end query 1 in stream 0 using template query23.tpl
