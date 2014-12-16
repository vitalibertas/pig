register /home/user/pig/json-20090211.jar;
register /home/user/pig/bacon.jar;

%declare PROCESSYEAR `date --date="$PROCESSTIME" +%Y`
%declare MONTHNO0 `date --date="$PROCESSTIME" +%-m`
%declare PROCESSDAY `date --date="$PROCESSTIME" +%-d`
%declare YESTERDAY `date --date="$PROCESSTIME yesterday" +%-d`


wordpressKafka = LOAD '/feed-data/feed-name/year=$PROCESSYEAR/month=$MONTHNO0/day=$YESTERDAY'
                      USING parquet.pig.ParquetLoader();


-- parse the 'json' field of the kafka message
wordpressData = FOREACH wordpressKafka GENERATE org.archive.bacon.FromJSON(json) AS json:[];

-- pull fields out of JSON
pluginMap = FOREACH wordpressData {
  eventTime = ToDate((chararray)json#'event_time_iso8601');
  lastPost = ToDate((chararray)json#'last_post');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMdd');
  GENERATE
   (chararray)json#'prnm'                                AS product
  ,(chararray)json#'fqdn'                                AS hostname
  ,eventTime                                             AS event_time
  ,(chararray)json#'db_host'                             AS database_host
  ,(chararray)json#'db_name'                             AS database_name
  ,(chararray)json#'db_port'                             AS database_port
  ,(chararray)json#'db_socket'                           AS database_socket
  ,(chararray)json#'db_version'                          AS database_version
  ,lastPost                                              AS last_post
  ,(int)json#'num_comments'                              AS number_comments
  ,(int)json#'num_posts'                                 AS number_posts
  ,(int)json#'num_users'                                 AS number_users
  ,(chararray)json#'options_table'                       AS options_table
  ,FLATTEN(json#'plugins')                               AS plugins_map
  ,(chararray)json#'theme'                               AS theme
  ,(chararray)json#'url'                                 AS url
  ,(chararray)json#'version'                             AS version
  ,jobId                                                 AS job_id
;}

wordpress = FOREACH pluginMap GENERATE
   product
  ,hostname
  ,event_time
  ,database_host
  ,database_name
  ,database_port
  ,database_socket
  ,database_version
  ,last_post
  ,number_comments
  ,number_posts
  ,number_users
  ,options_table
  ,(map[(pnum:chararray,pname:chararray)])plugins_map
  ,theme
  ,url
  ,version
  ,job_id
;

wordpressDay = FILTER wordpress BY (HoursBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                AND (HoursBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 24);

STORE wordpressDay INTO 'database.table' USING org.apache.hive.hcatalog.pig.HCatStorer();
