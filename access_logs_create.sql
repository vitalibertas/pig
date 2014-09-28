/*
do-as-hosting hive <script>
*/

USE some_database;

CREATE TABLE access_logs(
  product          STRING     COMMENT 'Product Name'
 ,account_id       STRING     COMMENT 'Customer Id'
 ,hostname         STRING     COMMENT 'Fully Qualified Domain Name'
 ,client_ip        STRING     COMMENT 'eg. 50.62.161.5'
 ,event_time       TIMESTAMP
 ,method           STRING     COMMENT 'eg. GET,POST'
 ,uri_request      STRING     COMMENT 'eg. http://nym1.adnxs.pw/wp-content/themes/broadway/custom.css'
 ,http_version     STRING     COMMENT 'eg. HTTP/1.1'
 ,response_code    INT        COMMENT 'eg. 200'
 ,response_size    BIGINT     COMMENT 'eg. 20'
 ,referrer         STRING     COMMENT 'eg. http://nym34.adnxs.pw/'
 ,user_agent       STRING     COMMENT 'eg. Mozilla/4.0'
 ,request_duration BIGINT     COMMENT 'eg. 0.00075078'
 ,additional       STRING     COMMENT 'Anything else in the log line')
COMMENT 'Populated from the collector for some_database'
PARTITIONED BY (job_id STRING)
STORED AS orc;

/*
https://confluence.int.godaddy.com/display/hostingInt/Linux+General
https://confluence.int.godaddy.com/pages/viewpage.action?title=Data+Collection&spaceKey=CICDHOST

What's the difference between STORED AS orc vs. using text?
http://hortonworks.com/blog/orcfile-in-hdp-2-better-compression-better-performance/
*/
