-- start query 1 in stream 0 using template query28.tpl

SELECT *
FROM
    (SELECT avg(ss_list_price) B1_LP ,
            count(ss_list_price) B1_CNT ,
            count(DISTINCT ss_list_price) B1_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 0 AND 5
         AND (ss_list_price BETWEEN 11 AND 11+10
              OR ss_coupon_amt BETWEEN 460 AND 460+1000
              OR ss_wholesale_cost BETWEEN 14 AND 14+20)) B1,

    (SELECT avg(ss_list_price) B2_LP ,
            count(ss_list_price) B2_CNT ,
            count(DISTINCT ss_list_price) B2_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 6 AND 10
         AND (ss_list_price BETWEEN 91 AND 91+10
              OR ss_coupon_amt BETWEEN 1430 AND 1430+1000
              OR ss_wholesale_cost BETWEEN 32 AND 32+20)) B2,

    (SELECT avg(ss_list_price) B3_LP ,
            count(ss_list_price) B3_CNT ,
            count(DISTINCT ss_list_price) B3_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 11 AND 15
         AND (ss_list_price BETWEEN 66 AND 66+10
              OR ss_coupon_amt BETWEEN 920 AND 920+1000
              OR ss_wholesale_cost BETWEEN 4 AND 4+20)) B3,

    (SELECT avg(ss_list_price) B4_LP ,
            count(ss_list_price) B4_CNT ,
            count(DISTINCT ss_list_price) B4_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 16 AND 20
         AND (ss_list_price BETWEEN 142 AND 142+10
              OR ss_coupon_amt BETWEEN 3054 AND 3054+1000
              OR ss_wholesale_cost BETWEEN 80 AND 80+20)) B4,

    (SELECT avg(ss_list_price) B5_LP ,
            count(ss_list_price) B5_CNT ,
            count(DISTINCT ss_list_price) B5_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 21 AND 25
         AND (ss_list_price BETWEEN 135 AND 135+10
              OR ss_coupon_amt BETWEEN 14180 AND 14180+1000
              OR ss_wholesale_cost BETWEEN 38 AND 38+20)) B5,

    (SELECT avg(ss_list_price) B6_LP ,
            count(ss_list_price) B6_CNT ,
            count(DISTINCT ss_list_price) B6_CNTD
     FROM store_sales
     WHERE ss_quantity BETWEEN 26 AND 30
         AND (ss_list_price BETWEEN 28 AND 28+10
              OR ss_coupon_amt BETWEEN 2513 AND 2513+1000
              OR ss_wholesale_cost BETWEEN 42 AND 42+20)) B6
LIMIT 100;

-- end query 1 in stream 0 using template query28.tpl