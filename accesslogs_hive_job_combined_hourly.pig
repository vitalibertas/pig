register /home/user/pig/json-20090211.jar;
register /home/user/pig/bacon.jar;

%declare PROCESSYEAR `date --date="$PROCESSTIME" +%Y`
%declare MONTHNO0 `date --date="$PROCESSTIME" +%-m`
%declare PROCESSDAY `date --date="$PROCESSTIME" +%-d`
%declare YESTERYEAR `date --date="$PROCESSTIME yesterday" +%Y`
%declare YESTERMONO0 `date --date="$PROCESSTIME yesterday" +%-m`
%declare YESTERDAY `date --date="$PROCESSTIME yesterday" +%-d`
%declare PROCESSHOUR `date --date="$PROCESSTIME" +%k`


accessLogsKafka = LOAD '/feed-data/feed-name/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY'
                    USING parquet.pig.ParquetLoader();

-- Wrap the 'json' field of the kafka message to deal with the extra [] in the feed.
wrappedLog = FOREACH accessLogsKafka GENERATE CONCAT('{"jsonData":',CONCAT(json,'}')) AS json;

-- Pull out the json.
accessLogsMap = FOREACH wrappedLog GENERATE org.archive.bacon.FromJSON(json) AS json:[];

-- Flatten out jsonData so we can get access to the map.
accessLogs = FOREACH accessLogsMap GENERATE FLATTEN (json#'jsonData') AS logs;

-- Pull out the fields we're going to store.
accessTuple = FOREACH accessLogs {
  eventTime = ToDate((chararray)logs#'event_time_iso8601');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  GENERATE
   (chararray)logs#'prnm'        AS product
  ,(chararray)logs#'cid'         AS account_id
  ,(chararray)logs#'fqdn'        AS hostname
  ,(chararray)logs#'uip'         AS client_ip
  ,(datetime)eventTime           AS event_time
  ,(chararray)logs#'http_method' AS method
  ,(chararray)logs#'dl'          AS uri_request
  ,(chararray)logs#'http_ver'    AS http_version
  ,(int)logs#'res_code'          AS response_code
  ,(long)logs#'res_size'         AS response_size
  ,(chararray)logs#'dr'          AS referrer
  ,(chararray)logs#'us'          AS user_agent
  ,(long)logs#'duration'         AS request_duration
  ,(chararray)logs#'residual'    AS additional
  ,(chararray)jobId              AS job_id
;}

accessHour = FILTER accessTuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);


-- Managed Word Press
cacheLogsKafka = LOAD '/feed-data/dcr/dcr.hosting_cache_access/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY'
                    USING parquet.pig.ParquetLoader();

cacheLogs = FOREACH cacheLogsKafka GENERATE org.archive.bacon.FromJSON(json) AS json:[];

cacheTuple = FOREACH cacheLogs {
  eventTime = ToDate((chararray)json#'event_time_iso8601');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  GENERATE
   (chararray)json#'prnm'        AS product
  ,(chararray)json#'xid'         AS account_id
  ,(chararray)json#'fqdn'        AS hostname
  ,(chararray)json#'uip'         AS client_ip
  ,(datetime)eventTime           AS event_time
  ,(chararray)json#'http_method' AS method
  ,(chararray)json#'dl'          AS uri_request
  ,(chararray)json#'http_ver'    AS http_version
  ,(int)json#'res_code'          AS response_code
  ,(long)json#'res_size'         AS response_size
  ,(chararray)json#'dr'          AS referrer
  ,(chararray)json#'ua'          AS user_agent
  ,(long)json#'duration'         AS request_duration
  ,(chararray)json#'residual'    AS additional
  ,(chararray)jobId              AS job_id
;}

cacheHour = FILTER cacheTuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);


-- Linux 2g
apache2gFeed = LOAD '/hosting/access_logs/apache_2gh/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY/hour=$PROCESSHOUR'
                    USING AvroStorage('{"type":"record","name":"LogEntry","fields":[
                                {"name": "client_ip", "type": ["string", "null"]},
                                {"name": "client_id", "type": ["string", "null"]},
                                {"name": "remote_user", "type": ["string", "null"]},
                                {"name": "request_time", "type": ["string", "null"]},
                                {"name": "method", "type": ["string", "null"]},
                                {"name": "uri_stem", "type": ["string", "null"]},
                                {"name": "uri_query", "type": ["string", "null"]},
                                {"name": "http_version", "type": ["string", "null"]},
                                {"name": "status", "type": ["int", "null"]},
                                {"name": "bytes_sent", "type": "long"},
                                {"name": "bytes_received", "type": "long"},
                                {"name": "referrer", "type": ["string", "null"]},
                                {"name": "user_agent", "type": ["string", "null"]},
                                {"name": "keepalive_reqs", "type": ["int", "null"]},
                                {"name": "handler", "type": ["string", "null"]},
                                {"name": "file_name", "type": ["string", "null"]},
                                {"name": "proc_time_us", "type": ["long", "null"]},
                                {"name": "customer_id", "type": ["string", "null"]},
                                {"name": "site_name", "type": ["string", "null"]},
                                {"name": "server_name", "type": ["string", "null"]},
                                {"name": "server_port", "type": ["int", "null"]},
                                {"name": "log_source", "type": ["string", "null"]},
                                {"name": "log_type", "type": ["string", "null"]},
                                {"name": "record_uniq", "type": ["string", "null"]}]}');

apache2gTuple = FOREACH apache2gFeed {
  eventTime = ToDate((chararray)request_time, 'yyyy-MM-dd HH:mm:ss');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  uriRequest = CONCAT('http://', CONCAT(uri_stem, uri_query));
  GENERATE
   '2gh-linux'             AS product
  ,(chararray)customer_id  AS account_id
  ,(chararray)server_name  AS hostname
  ,(chararray)client_ip    AS client_ip
  ,(datetime)eventTime     AS event_time
  ,(chararray)method       AS method
  ,(chararray)uriRequest   AS uri_request
  ,(chararray)http_version AS http_version
  ,(int)status             AS response_code
  ,(long)bytes_sent        AS response_size
  ,(chararray)referrer     AS referrer
  ,(chararray)user_agent   AS user_agent
  ,NULL                    AS request_duration
  ,NULL                    AS additional
  ,(chararray)jobId        AS job_id
;}

apache2gHour = FILTER apache2gTuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);


-- Linux 4g
apache4gFeed = LOAD '/hosting/access_logs/apache_4gh/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY/hour=$PROCESSHOUR'
                    USING AvroStorage('{"type":"record","name":"LogEntry","fields":[
                                {"name": "client_ip", "type": ["string", "null"]},
                                {"name": "client_id", "type": ["string", "null"]},
                                {"name": "remote_user", "type": ["string", "null"]},
                                {"name": "request_time", "type": ["string", "null"]},
                                {"name": "method", "type": ["string", "null"]},
                                {"name": "uri_stem", "type": ["string", "null"]},
                                {"name": "uri_query", "type": ["string", "null"]},
                                {"name": "http_version", "type": ["string", "null"]},
                                {"name": "status", "type": ["int", "null"]},
                                {"name": "bytes_sent", "type": "long"},
                                {"name": "bytes_received", "type": "long"},
                                {"name": "referrer", "type": ["string", "null"]},
                                {"name": "user_agent", "type": ["string", "null"]},
                                {"name": "keepalive_reqs", "type": ["int", "null"]},
                                {"name": "handler", "type": ["string", "null"]},
                                {"name": "file_name", "type": ["string", "null"]},
                                {"name": "proc_time_us", "type": ["long", "null"]},
                                {"name": "customer_id", "type": ["string", "null"]},
                                {"name": "site_name", "type": ["string", "null"]},
                                {"name": "server_name", "type": ["string", "null"]},
                                {"name": "server_port", "type": ["int", "null"]},
                                {"name": "log_source", "type": ["string", "null"]},
                                {"name": "log_type", "type": ["string", "null"]},
                                {"name": "record_uniq", "type": ["string", "null"]}]}');

apache4gTuple = FOREACH apache4gFeed {
  eventTime = ToDate((chararray)request_time, 'yyyy-MM-dd HH:mm:ss');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  uriRequest = CONCAT('http://', CONCAT(uri_stem, uri_query));
  GENERATE
  '4gh-linux'             AS product
  ,(chararray)customer_id  AS account_id
  ,(chararray)server_name  AS hostname
  ,(chararray)client_ip    AS client_ip
  ,(datetime)eventTime     AS event_time
  ,(chararray)method       AS method
  ,(chararray)uriRequest   AS uri_request
  ,(chararray)http_version AS http_version
  ,(int)status             AS response_code
  ,(long)bytes_sent        AS response_size
  ,(chararray)referrer     AS referrer
  ,(chararray)user_agent   AS user_agent
  ,NULL                    AS request_duration
  ,NULL                    AS additional
  ,(chararray)jobId        AS job_id
;}

apache4gHour = FILTER apache4gTuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);



iis6Feed = LOAD '/hosting/access_logs/windows_iis6/year=$YESTERYEAR/month=$YESTERMONO0/day=$YESTERDAY/hour=$PROCESSHOUR'
                    USING AvroStorage('{"type":"record","name":"LogEntry","fields":[
                                {"name": "client_ip", "type": ["string", "null"]},
                                {"name": "client_id", "type": ["string", "null"]},
                                {"name": "remote_user", "type": ["string", "null"]},
                                {"name": "request_time", "type": ["string", "null"]},
                                {"name": "method", "type": ["string", "null"]},
                                {"name": "uri_stem", "type": ["string", "null"]},
                                {"name": "uri_query", "type": ["string", "null"]},
                                {"name": "http_version", "type": ["string", "null"]},
                                {"name": "status", "type": ["int", "null"]},
                                {"name": "bytes_sent", "type": "long"},
                                {"name": "bytes_received", "type": "long"},
                                {"name": "referrer", "type": ["string", "null"]},
                                {"name": "user_agent", "type": ["string", "null"]},
                                {"name": "keepalive_reqs", "type": ["int", "null"]},
                                {"name": "handler", "type": ["string", "null"]},
                                {"name": "file_name", "type": ["string", "null"]},
                                {"name": "proc_time_us", "type": ["long", "null"]},
                                {"name": "customer_id", "type": ["string", "null"]},
                                {"name": "site_name", "type": ["string", "null"]},
                                {"name": "server_name", "type": ["string", "null"]},
                                {"name": "server_port", "type": ["int", "null"]},
                                {"name": "log_source", "type": ["string", "null"]},
                                {"name": "log_type", "type": ["string", "null"]},
                                {"name": "record_uniq", "type": ["string", "null"]}]}');


iis6Tuple = FOREACH iis6Feed {
  eventTime = ToDate((chararray)request_time, 'yyyy-MM-dd HH:mm:ss');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  uriRequest = CONCAT(uri_stem, uri_query);
  GENERATE
  '2gh-windows'            AS product
  ,(chararray)customer_id  AS account_id
  ,(chararray)server_name  AS hostname
  ,(chararray)client_ip    AS client_ip
  ,(datetime)eventTime     AS event_time
  ,(chararray)method       AS method
  ,(chararray)uriRequest   AS uri_request
  ,(chararray)http_version AS http_version
  ,(int)status             AS response_code
  ,(long)bytes_sent        AS response_size
  ,(chararray)referrer     AS referrer
  ,(chararray)user_agent   AS user_agent
  ,NULL                    AS request_duration
  ,NULL                    AS additional
  ,(chararray)jobId        AS job_id
;}

iis6Hour = FILTER iis6Tuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);



iis7Feed = LOAD '/hosting/access_logs/windows_iis7/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY/hour=$PROCESSHOUR'
                    USING AvroStorage('{"type":"record","name":"LogEntry","fields":[
                                {"name": "client_ip", "type": ["string", "null"]},
                                {"name": "client_id", "type": ["string", "null"]},
                                {"name": "remote_user", "type": ["string", "null"]},
                                {"name": "request_time", "type": ["string", "null"]},
                                {"name": "method", "type": ["string", "null"]},
                                {"name": "uri_stem", "type": ["string", "null"]},
                                {"name": "uri_query", "type": ["string", "null"]},
                                {"name": "http_version", "type": ["string", "null"]},
                                {"name": "status", "type": ["int", "null"]},
                                {"name": "bytes_sent", "type": "long"},
                                {"name": "bytes_received", "type": "long"},
                                {"name": "referrer", "type": ["string", "null"]},
                                {"name": "user_agent", "type": ["string", "null"]},
                                {"name": "keepalive_reqs", "type": ["int", "null"]},
                                {"name": "handler", "type": ["string", "null"]},
                                {"name": "file_name", "type": ["string", "null"]},
                                {"name": "proc_time_us", "type": ["long", "null"]},
                                {"name": "customer_id", "type": ["string", "null"]},
                                {"name": "site_name", "type": ["string", "null"]},
                                {"name": "server_name", "type": ["string", "null"]},
                                {"name": "server_port", "type": ["int", "null"]},
                                {"name": "log_source", "type": ["string", "null"]},
                                {"name": "log_type", "type": ["string", "null"]},
                                {"name": "record_uniq", "type": ["string", "null"]}]}');


iis7Tuple = FOREACH iis7Feed {
  eventTime = ToDate((chararray)request_time, 'yyyy-MM-dd HH:mm:ss');
  jobId = ToString(ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z'), 'yyyyMMddHH');
  uriRequest = CONCAT('http://', CONCAT(site_name, CONCAT(uri_stem, uri_query)));
  GENERATE
  '4gh-windows'            AS product
  ,(chararray)customer_id  AS account_id
  ,(chararray)server_name  AS hostname
  ,(chararray)client_ip    AS client_ip
  ,(datetime)eventTime     AS event_time
  ,(chararray)method       AS method
  ,(chararray)uriRequest   AS uri_request
  ,(chararray)http_version AS http_version
  ,(int)status             AS response_code
  ,(long)bytes_sent        AS response_size
  ,(chararray)referrer     AS referrer
  ,(chararray)user_agent   AS user_agent
  ,NULL                    AS request_duration
  ,NULL                    AS additional
  ,(chararray)jobId        AS job_id
;}

iis7Hour = FILTER iis7Tuple BY (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) >= 0)
                                 AND (MinutesBetween(event_time, ToDate('$PROCESSTIME', 'yyyy-MM-dd HH:mm Z')) < 60);


accessMerge = UNION ONSCHEMA accessHour, cacheHour, apache2gHour, apache4gHour, iis6Hour, iis7Hour;

STORE accessMerge INTO 'database.table' USING org.apache.hive.hcatalog.pig.HCatStorer();
