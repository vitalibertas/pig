/*
do-as-hosting pig -useHCatalog <script>
*/

register /home/cbrickner/pig/json-20090211.jar;
register /home/cbrickner/pig/bacon.jar;

%declare PROCESSYEAR `date --date="$PROCESSTIME" +%Y`
%declare MONTHNO0 `date --date="$PROCESSTIME" +%-m`
%declare PROCESSDAY `date --date="$PROCESSTIME" +%-d`
%declare PROCESSHOUR `date --date="$PROCESSTIME" +%k`


accessLogsKafka = LOAD '/feed-data/dcr/dcr.hosting_access_logs/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY'
                    USING parquet.pig.ParquetLoader();

cacheLogsKafka = LOAD '/feed-data/dcr/dcr.hosting_cache_access/year=$PROCESSYEAR/month=$MONTHNO0/day=$PROCESSDAY'
                    USING parquet.pig.ParquetLoader();

-- Wrap the 'json' field of the kafka message to deal with the extra [] in the feed.
wrappedLog = FOREACH accessLogsKafka GENERATE CONCAT('{"jsonData":',CONCAT(json,'}')) AS json;

-- Pull out the json.
accessLogsMap = FOREACH wrappedLog GENERATE org.archive.bacon.FromJSON(json) AS json:[];

cacheLogs = FOREACH cacheLogsKafka GENERATE org.archive.bacon.FromJSON(json) AS json:[];

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
accessMerge = UNION ONSCHEMA accessHour, cacheHour;

STORE accessMerge INTO 'hosting_stats.access_logs' USING org.apache.hive.hcatalog.pig.HCatStorer();
