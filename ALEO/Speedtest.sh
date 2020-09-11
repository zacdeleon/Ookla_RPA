#!/bin/ksh

create_agg()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE if not exists LTE_ATHOME_APN (
        TIMESTAMP DATE,
        IMSI int,
        Cust_Acc_No int,
        PLAN int,
        Centrum_DL_Ookla varchar(20),
        >30%_fulfillment Text,
        Ookla_DL_TPUT_(Mbps) float,
        Ookla_UL_TPUT(Mbps) float,
        Avg._RAN_LATENCY int,
        Avg._INTERNET_LATENCY int,
        Distinct_count_of_Ookla_DL_TPUT_(Mbps) int,
        Max._Ookla_DL_TPUT_(Mbps) float,
        Max._Ookla_UL_TPUT(Mbps) float,
        Min._Ookla_DL_TPUT_(Mbps) float,
        Min._Ookla_UL_TPUT(Mbps) float,
        FLAG int(1),
        EXTRACT_FLAG int(1),
        CONSTRAINT LTE_ATHOME_APN UNIQUE (TIMESTAMP,IMSI)
);

EOFEOF
}

create_agg2()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE if not exists LTE_HOMEBROADBAND_APN (
        TIMESTAMP DATE,
        IMSI int,
        Centrum_DL varchar(20),
        >2mbps Text,
        Ookla_DL_TPUT_(Mbps) float,
        Ookla_UL_TPUT(Mbps) float,
        Avg._RAN_LATENCY int,
        Avg._INTERNET_LATENCY int,
        Distinct_count_of_Ookla_DL_TPUT_(Mbps) int,
        Max._Ookla_DL_TPUT_(Mbps) float,
        Max._Ookla_UL_TPUT(Mbps) float,
        Min._Ookla_DL_TPUT_(Mbps) float,
        Min._Ookla_UL_TPUT(Mbps) float,
        FLAG int(1),
        EXTRACT_FLAG int(1),
        CONSTRAINT LTE_HOMEBROADBAND_APN UNIQUE (TIMESTAMP,IMSI)
);

EOFEOF
}

agg_script()
{
${MYSQL} KJOHN << EOFEOF

insert into LTE_ATHOME_APN
SELECT TIMESTAMP, IMSI,Cust_Acc_No,PLAN,
Ookla_DL_TPUT_()
(CASE
  when Ookla_DL_TPUT_(Mbps) < 0.512 and Ookla_DL_TPUT_(Mbps) != 0 then '< 512Kbps'
  when Ookla_DL_TPUT_(Mbps) > 1 and Ookla_DL_TPUT_(Mbps) >= 0.512 then '512Kbps - 1Mbps'
  when Ookla_DL_TPUT_(Mbps) > 2 and Ookla_DL_TPUT_(Mbps) >= 1 then '1Mbps - 2Mbps' 
  when Ookla_DL_TPUT_(Mbps) > 3 and Ookla_DL_TPUT_(Mbps) >= 2 then '2Mbps - 3Mbps'
  when Ookla_DL_TPUT_(Mbps) > 4 and Ookla_DL_TPUT_(Mbps) >= 3 then '3Mbps - 4Mbps'
  when Ookla_DL_TPUT_(Mbps) > 5 and Ookla_DL_TPUT_(Mbps) >= 4 then '4Mbps - 5Mbps'
  when Ookla_DL_TPUT_(Mbps) != 0 and Ookla_DL_TPUT_(Mbps) >= 5 then '5Mbps'
END) as Centrum_DL_Ookla,



(CASE
  when Ookla_DL_TPUT_(Mbps) > (PLAN*0.3) then 'YES' else 'NO'
END) as >30%_fulfillment,




EOFEOF
}

agg_script2()
{
${MYSQL} KJOHN << EOFEOF

insert into LTE_ATHOME_APN
SELECT TIMESTAMP, IMSI,Cust_Acc_No,PLAN,
(case
  when  
)



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
