#!/bin/ksh

create_agg()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE if not exists FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_SPEED_CNT
(
Timestamp date,
CELL_REGION Varchar (20),
Brand Varchar (20),
QOS_Tier Varchar (20),
cnt_Below_512_Kbps Int,
cnt_512_700_Kbps Int,
cnt_700_Kbps_1p12_Mbps Int,
cnt_1p12_1p5_Mbps Int,
cnt_1p5_2Mbps Int,
cnt_2_2p5_Mbps Int,
cnt_2p5_5_Mbps Int,
cnt_Above_5_Mbps Int,
cnt_Null_values Int,
cnt_Total_wo_Null Int,
FLAG Int(1),
EXTRACT_FLAG Int(1),
CONSTRAINT FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_SPEED_CNT_UNQ UNIQUE (TIMESTAMP,CELL_REGION,Brand,QOS_Tier)
);

EOFEOF
}

create_agg_ratio()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE if not exists FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_RATIO
(
Timestamp date,
CELL_REGION Varchar (20),
Brand Varchar (20),
QOS_Tier Varchar (20),
cnt_Below_512_Kbps Int,
cnt_512_700_Kbps Int,
cnt_700_Kbps_1p12_Mbps Int,
cnt_1p12_1p5_Mbps Int,
cnt_1p5_2Mbps Int,
cnt_2_2p5_Mbps Int,
cnt_2p5_5_Mbps Int,
cnt_Above_5_Mbps Int,
cnt_Null_values Int,
cnt_Total_wo_Null Int,
dl_tput_700kbps_ratio Float,
dl_tput_1mbps_ratio Float,
dl_tput_2mbps_ratio Float,
dl_tput_2p5mbps_ratio Float,
FLAG Int(1),
EXTRACT_FLAG Int(1)
);

EOFEOF
}

agg_script()
{
${MYSQL} KJOHN << EOFEOF

insert into FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_SPEED_CNT
select date(Timestamp) AS Timestamp, 'ALL' as CELL_REGION,
  (case
        when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
        when a.apn='LTE athome APN' then 'Athome'
        else b.brand
      END) as Brand,
  (case
        when Bearer_QCI='6' and Bearer_PL='1' and rat_type_name='LTE' then 'Platinum'
        when Bearer_QCI='7' and Bearer_PL='6' and rat_type_name='LTE' then 'Gold'
        when Bearer_QCI='8' and Bearer_PL='11' and rat_type_name='LTE' then 'Silver'
        when Bearer_QCI='9' and Bearer_PL='6' and rat_type_name='LTE' then 'Bronze'
        else 'Others'
          END
        ) as QOS_Tier,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps <'512' and AVG_Effective_Tput_DL_Kbps > 0 and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_Below_512_Kbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='512' and AVG_Effective_Tput_DL_Kbps < '700'  and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_512_700_Kbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='700' and AVG_Effective_Tput_DL_Kbps < '1120' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_700_Kbps_1p12_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='1120' and AVG_Effective_Tput_DL_Kbps < '1500' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_1p12_1p5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='1500' and AVG_Effective_Tput_DL_Kbps < '2000' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_1p5_2Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='2000' and AVG_Effective_Tput_DL_Kbps < '2500' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_2_2p5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='2500' and AVG_Effective_Tput_DL_Kbps < '5000' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_2p5_5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='5000' then imsi
      END)) as cnt_Above_5_Mbps ,
  count(distinct(case
  when AVG_Effective_Tput_DL_Kbps is NULL and AVG_Effective_Tput_DL_Kbps=0 then  imsi
  end)) as cnt_Null_values ,
  count(distinct(imsi)) as cnt_Total_wo_Null, 0 as FLAG, 0 as EXTRACT_FLAG
     from FACEBOOK_VIDEO_IMSI a, msisdn_brand b
      WHERE substr(a.msisdn,1,7) = b.msisdn and date(timestamp) = date(NOW()) - INTERVAL 1 DAY
      and rat_type_name='LTE'
      group by date(timestamp), 'ALL',
      (case
        when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
        when a.apn='LTE athome APN' then 'Athome'
        else b.brand
      END),
      (case
        when Bearer_QCI='6' and Bearer_PL='1' and rat_type_name='LTE' then 'Platinum'
        when Bearer_QCI='7' and Bearer_PL='6' and rat_type_name='LTE' then 'Gold'
        when Bearer_QCI='8' and Bearer_PL='11' and rat_type_name='LTE' then 'Silver'
        when Bearer_QCI='9' and Bearer_PL='6' and rat_type_name='LTE' then 'Bronze'
        else 'Others'
          END
        )
  union ALL
  select date(Timestamp) AS Timestamp,CELL_REGION,
  (case
        when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
        when a.apn='LTE athome APN' then 'Athome'
        else b.brand
      END) as Brand,
  (case
        when Bearer_QCI='6' and Bearer_PL='1' and rat_type_name='LTE' then 'Platinum'
        when Bearer_QCI='7' and Bearer_PL='6' and rat_type_name='LTE' then 'Gold'
        when Bearer_QCI='8' and Bearer_PL='11' and rat_type_name='LTE' then 'Silver'
        when Bearer_QCI='9' and Bearer_PL='6' and rat_type_name='LTE' then 'Bronze'
        else 'Others'
          END
        ) as QOS_Tier,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps <'512' and AVG_Effective_Tput_DL_Kbps >0 and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_Below_512_Kbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='512.' and AVG_Effective_Tput_DL_Kbps < '700' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_512_700_Kbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='700' and AVG_Effective_Tput_DL_Kbps < '1120' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_700_Kbps_1p12_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='1120' and AVG_Effective_Tput_DL_Kbps < '1500' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_1p12_1p5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='1500' and AVG_Effective_Tput_DL_Kbps < '2000' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_1p5_2Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='2000' and AVG_Effective_Tput_DL_Kbps < '2500' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_2_2p5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='2500' and AVG_Effective_Tput_DL_Kbps < '5000' and AVG_Effective_Tput_DL_Kbps is not NULL then imsi
      END)) as cnt_2p5_5_Mbps ,
  count(distinct(case
      when AVG_Effective_Tput_DL_Kbps >='5000' then imsi
      END)) as cnt_Above_5_Mbps ,
  count(distinct(case
  when AVG_Effective_Tput_DL_Kbps is NULL and AVG_Effective_Tput_DL_Kbps=0 then  imsi
  end)) as cnt_Null_values ,
   count(distinct(imsi)) as cnt_Total_wo_Null, 0 as FLAG, 0 as EXTRACT_FLAG
     from FACEBOOK_VIDEO_IMSI a, msisdn_brand b
      WHERE substr(a.msisdn,1,7) = b.msisdn and date(timestamp) = date(NOW()) - INTERVAL 1 DAY
      and rat_type_name='LTE'
      group by date(timestamp), CELL_REGION,
      (case
        when a.apn='LTE HomeBroadband APN' then 'HomeBroadband'
        when a.apn='LTE athome APN' then 'Athome'
        else b.brand
      END),
      (case
        when Bearer_QCI='6' and Bearer_PL='1' and rat_type_name='LTE' then 'Platinum'
        when Bearer_QCI='7' and Bearer_PL='6' and rat_type_name='LTE' then 'Gold'
        when Bearer_QCI='8' and Bearer_PL='11' and rat_type_name='LTE' then 'Silver'
        when Bearer_QCI='9' and Bearer_PL='6' and rat_type_name='LTE' then 'Bronze'
        else 'Others'
          END
        );

EOFEOF
}

agg_script_ratio()
{
${MYSQL} KJOHN << EOFEOF

insert into FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_RATIO
select Timestamp,CELL_REGION,Brand,QOS_Tier,
cnt_Below_512_Kbps,
cnt_512_700_Kbps,
cnt_700_Kbps_1p12_Mbps,
cnt_1p12_1p5_Mbps,
cnt_1p5_2Mbps,
cnt_2_2p5_Mbps,
cnt_2p5_5_Mbps,
cnt_Above_5_Mbps,
cnt_Null_values,
(cnt_Below_512_Kbps+cnt_512_700_Kbps+cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps) as cnt_Total_wo_Null,
(nullif(cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0))*100/
(nullif(cnt_Below_512_Kbps+cnt_512_700_Kbps+cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0)) as dl_tput_700kbps_ratio,
(nullif(cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0))*100/
(nullif(cnt_Below_512_Kbps+cnt_512_700_Kbps+cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0)) as dl_tput_1mbps_ratio,
(nullif(cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0))*100/
(nullif(cnt_Below_512_Kbps+cnt_512_700_Kbps+cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0)) as dl_tput_2mbps_ratio,
(nullif(cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0))*100/
(nullif(cnt_Below_512_Kbps+cnt_512_700_Kbps+cnt_700_Kbps_1p12_Mbps+cnt_1p12_1p5_Mbps+cnt_1p5_2Mbps+cnt_2_2p5_Mbps+cnt_2p5_5_Mbps+cnt_Above_5_Mbps,0)) as dl_tput_2p5mbps_ratio,
0 as FLAG, 0 as EXTRACT_FLAG
from FACEBOOK_VIDEO_BRAND_AGG_DAILY_DL_TPUT_SPEED_CNT
where date(timestamp) = date(NOW()) - INTERVAL 1 DAY;

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

echo "`date` Running Create Aggregate Table if not exist for FACEBOOK VIDEO per Brand with QOS Tier DL Tput Count per speed..."
create_agg

echo "`date` Done. Running Aggregation..."
agg_script

echo "`date` Done."

echo "`date` Running Create Aggregate Table if not exist for FACEBOOK VIDEO per Brand with QOS Tier DL Tput ratio..."
create_agg_ratio

echo "`date` Done. Running Aggregation..."
agg_script_ratio

echo "`date` Done."



#END PROGRAM

