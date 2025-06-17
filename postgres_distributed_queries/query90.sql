-- start query 1 in stream 0 using template query90.tpl

SELECT cast(amc AS decimal(15, 4))/cast(pmc AS decimal(15, 4)) am_pm_ratio
FROM
    (SELECT count(*) amc
     FROM web_sales,
          household_demographics,
          time_dim,
          web_page
     WHERE ws_sold_time_sk = time_dim.t_time_sk
         AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         AND ws_web_page_sk = web_page.wp_web_page_sk
         AND time_dim.t_hour BETWEEN 6 AND 6+1
         AND household_demographics.hd_dep_count = 8
         AND web_page.wp_char_count BETWEEN 5000 AND 5200) AT,

    (SELECT count(*) pmc
     FROM web_sales,
          household_demographics,
          time_dim,
          web_page
     WHERE ws_sold_time_sk = time_dim.t_time_sk
         AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         AND ws_web_page_sk = web_page.wp_web_page_sk
         AND time_dim.t_hour BETWEEN 14 AND 14+1
         AND household_demographics.hd_dep_count = 8
         AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio
LIMIT 100;

-- end query 1 in stream 0 using template query90.tpl