#!/bin/ksh

create_agg()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE if not exists VIDEO_KQI_IMSI_AGG_DAILY (
        TIMESTAMP DATE,
        RAT_TYPE_NAME VARCHAR(20),
        CELL_REGION VARCHAR(10),
        Brand Varchar (20),
        MDN_AVG_TPUT_DL_K FLOAT,
        AVG_PLAY_VMOS_SCORE FLOAT,
        MDN_AVG_MIN_PTIME_SEC Float,
        CNT_PSTART_0_TO_1_SEC FLOAT,
        CNT_PSTART_1_TO_2_SEC FLOAT,
        CNT_PSTART_2_TO_3_SEC FLOAT,
        CNT_PSTART_3_TO_4_SEC FLOAT,
        CNT_PSTART_4_TO_5_SEC FLOAT,
        PLAYTIME_WITHIN_5S FLOAT,
        AVG_VIDEO_REBUF_RATIO FLOAT,
        AVG_P360_480_RATIO FLOAT,
        AVG_P720_1080_RATIO FLOAT,
        AVG_P1440_2160_RATIO FLOAT,
        P360_AND_ABOVE FLOAT,
        P720_AND_ABOVE FLOAT,
        CNT_TOTAL_VCDR_CNT FLOAT,
        UNIQ_IMSI_CNT FLOAT, 
        FLAG int(1),
        EXTRACT_FLAG int(1),
        CONSTRAINT VIDEO_KQI_IMSI_AGG_DAILY_UNQ UNIQUE (TIMESTAMP,RAT_TYPE_NAME,CELL_REGION,Brand)
);

EOFEOF
}

agg_script()
{
${MYSQL} KJOHN << EOFEOF

insert into VIDEO_KQI_IMSI_AGG_DAILY
SELECT TIMESTAMP, RAT_TYPE_NAME,CELL_REGION, Brand,
avg(mdn_aetdk) as MDN_AVG_TPUT_DL_K,
avg(nullif(AVG_PLAY_VMOS_SCORE,0)) AS AVG_PLAY_VMOS_SCORE,
avg(mdn_AMPS) AS MDN_AVG_MIN_PTIME_SEC,
sum(VCPS_0_TO_1_SEC) AS CNT_PSTART_0_TO_1_SEC,
sum(VCPS_1_TO_2_SEC) AS CNT_PSTART_1_TO_2_SEC,
sum(VCPS_2_TO_3_SEC) AS CNT_PSTART_2_TO_3_SEC,
sum(VCPS_3_TO_4_SEC) AS CNT_PSTART_3_TO_4_SEC,
sum(VCPS_4_TO_5_SEC) AS CNT_PSTART_4_TO_5_SEC,
nullif(sum(VCPS_0_TO_1_SEC+VCPS_1_TO_2_SEC+VCPS_2_TO_3_SEC+VCPS_3_TO_4_SEC+VCPS_4_TO_5_SEC),0)*100/
nullif(sum(VCPS_0_TO_1_SEC+VCPS_1_TO_2_SEC+VCPS_2_TO_3_SEC+VCPS_3_TO_4_SEC+VCPS_4_TO_5_SEC+
VCPS_5_TO_6_SEC+VCPS_6_TO_7_SEC+VCPS_7_TO_10_SEC+VCPS_ABOVE_10_SEC),0) AS PLAYTIME_WITHIN_5S,
avg(VIDEO_REBUF_RATIO) AS AVG_VIDEO_REBUF_RATIO,
avg(P360_480_RATIO) AS AVG_P360_480_RATIO,
avg(P720_1080_RATIO) AS AVG_P720_1080_RATIO,
avg(P1440_2160_RATIO) AS AVG_P1440_2160_RATIO,
AVG(P360_480_RATIO+P720_1080_RATIO+P1440_2160_RATIO) AS P360_AND_ABOVE,
AVG(P720_1080_RATIO+P1440_2160_RATIO) P720_AND_ABOVE,
sum(TOTAL_VIDEO_CDR_COUNT) AS CNT_TOTAL_VCDR_CNT,
sum(UNIQUE_IMSI_COUNT) as UNIQ_IMSI_CNT,
0 as FLAG, 0 as EXTRACT_FLAG
from
(
select date(TIMESTAMP) AS timestamp,
rat_type_name,
'ALL' AS cell_region,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END) as Brand,
   median(nullif(AVG_EFFECT_TPUT_DL_KBPS,0)) over (partition by date(timestamp),
   rat_type_name,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END)) AS mdn_aetdk,
    median(nullif(AMP_TIME_SEC,0)) over (partition by date(timestamp),
   rat_type_name,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END)) AS mdn_AMPS,
    AVG_PLAY_VMOS_SCORE,VCPS_0_TO_1_SEC,VCPS_1_TO_2_SEC,
    VCPS_2_TO_3_SEC,VCPS_3_TO_4_SEC,VCPS_4_TO_5_SEC,
   VCPS_5_TO_6_SEC,VCPS_6_TO_7_SEC,VCPS_7_TO_10_SEC,VCPS_ABOVE_10_SEC,
   VIDEO_REBUF_RATIO,P360_480_RATIO,P720_1080_RATIO,P1440_2160_RATIO,
   TOTAL_VIDEO_CDR_COUNT,UNIQUE_IMSI_COUNT
   from VIDEO_KQI_IMSI_E a, msisdn_brand b
    WHERE substr(a.msisdn,1,7) = b.msisdn
union ALL
select date(TIMESTAMP) AS timestamp,
rat_type_name,
cell_region,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END) as Brand,
   median(nullif(AVG_EFFECT_TPUT_DL_KBPS,0)) over (partition by date(timestamp),
   rat_type_name,
   cell_region,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END)) AS mdn_aetdk,
    median(nullif(AMP_TIME_SEC,0)) over (partition by date(timestamp),
   rat_type_name,
   cell_region,
    (case
      when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
      when a.apn='LTE athome APN' then 'Athome'
      else b.brand
    END)) AS mdn_AMPS,
    AVG_PLAY_VMOS_SCORE,VCPS_0_TO_1_SEC,VCPS_1_TO_2_SEC,
    VCPS_2_TO_3_SEC,VCPS_3_TO_4_SEC,VCPS_4_TO_5_SEC,
   VCPS_5_TO_6_SEC,VCPS_6_TO_7_SEC,VCPS_7_TO_10_SEC,VCPS_ABOVE_10_SEC,
   VIDEO_REBUF_RATIO,P360_480_RATIO,P720_1080_RATIO,P1440_2160_RATIO,
   TOTAL_VIDEO_CDR_COUNT,UNIQUE_IMSI_COUNT
   from VIDEO_KQI_IMSI_E a, msisdn_brand b
    WHERE substr(a.msisdn,1,7) = b.msisdn
    ) a 
    group by timestamp,rat_type_name,cell_region, Brand;


EOFEOF
}

#MAIN
petsa=`date "+%Y%m%d%H%M%S"`
BASE_DIR=/apps/DASHBOARD/RADCOM
CONFIG=${BASE_DIR}/config
BIN=${BASE_DIR}/bin
ALARM=${BASE_DIR}/alarm
LOG=${BASE_DIR}/logs
REPORT=${BASE_DIR}/report
WORKDIR=/apps/DASHBOARD/alarms/broadband
MYSQL=/apps/DASHBOARD/MYSQL/bin/mysqlmariadb

#CHECK IF DB IS UP
STATUS=`/apps/DASHBOARD/MYSQL/bin/check_db_status.sql`

if [ "${STATUS}" = "UP_AND_RUNNING" ]
then
echo "`date` MARIA DB is UP and RUNNING"
else
echo "`date` MARIA DB is DOWN...Exiting..."
exit 6
fi

echo "`date` Running Create Aggregate Table if not exist for Video KQI per IMSI..."
create_agg

echo "`date` Done. Running Aggregation..."
agg_script

echo "`date` Done."

#END PROGRAM
