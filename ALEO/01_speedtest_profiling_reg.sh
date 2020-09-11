#!/bin/ksh

notify()
{
echo "list{1002" >> ${TEMP}/send_ookla_data.dat
echo "sender{OOKLA" >> ${TEMP}/send_ookla_data.dat
cat ${TEMP}/ookla_notify.txt | sed -e 's/^/body{/g' >> ${TEMP}/send_ookla_data.dat
echo "control{1002" >> ${TEMP}/send_ookla_data.dat
mv ${TEMP}/send_ookla_data.dat /apps/DASHBOARD/TEXT/bowl
}

#SPEEDTEST PROFILING

speed_prof_dl()
{
/apps/DASHBOARD/MYSQL/bin/mysqlmariadb KJOHN << EOFEOF

select 'REGION,TIMESTAMP,TELCO,RAT,DEVICE_ID,DOWNLOAD_KBPS';

select Concat('"',IFNULL(REGION, ''),'","',IFNULL(TIMESTAMP, ''),'","', ifnull(telco, ''),'","', IFNULL(RAT, '') ,'","',ifnull(device_id, ''),'","',ifnull(round(DOWNLOAD_KBPS,3), ''),'"')
from OOKLA_REG_FILTER1_DL_V
where month(TIMESTAMP) = month(current_date - interval 1 MONTH);

EOFEOF
}

speed_prof_ul()
{
/apps/DASHBOARD/MYSQL/bin/mysqlmariadb KJOHN << EOFEOF

select 'REGION,TIMESTAMP,TELCO,RAT,DEVICE_ID,UPLOAD_KBPS';

select Concat('"',IFNULL(REGION, ''),'","',IFNULL(TIMESTAMP, ''),'","', ifnull(telco, ''),'","', IFNULL(RAT, '') ,'","',ifnull(device_id, ''),'","',ifnull(round(UPLOAD_KBPS,3), ''),'"')
from OOKLA_REG_FILTER1_UL_V
where month(TIMESTAMP) = month(current_date - interval 1 MONTH);

EOFEOF
}


speed_prof_dl_BH()
{
/apps/DASHBOARD/MYSQL/bin/mysqlmariadb KJOHN << EOFEOF

select 'REGION,TIMESTAMP,TELCO,RAT,DEVICE_ID,DOWNLOAD_KBPS';

select Concat('"',IFNULL(REGION, ''),'","',IFNULL(TIMESTAMP, ''),'","', ifnull(telco, ''),'","', IFNULL(RAT, '') ,'","',ifnull(device_id, ''),'","',ifnull(round(DOWNLOAD_KBPS,3), ''),'"')
from OOKLA_REG_FILTER1_BH_DL_V
where month(TIMESTAMP) = month(current_date - interval 1 MONTH);

EOFEOF
}

speed_prof_ul_BH()
{
/apps/DASHBOARD/MYSQL/bin/mysqlmariadb KJOHN << EOFEOF

select 'REGION,TIMESTAMP,TELCO,RAT,DEVICE_ID,UPLOAD_KBPS';

select Concat('"',IFNULL(REGION, ''),'","',IFNULL(TIMESTAMP, ''),'","', ifnull(telco, ''),'","', IFNULL(RAT, '') ,'","',ifnull(device_id, ''),'","',ifnull(round(UPLOAD_KBPS,3), ''),'"')
from OOKLA_REG_FILTER1_BH_UL_V
where month(TIMESTAMP) = month(current_date - interval 1 MONTH);

EOFEOF
}



petsa()
{
/apps/DASHBOARD/MYSQL/bin/mysqlmariadb KJOHN << EOFEOF

select 'DAYNO:',upper(date_format(date_format(curdate(),'%Y-%m-01') - interval 1 month,'%b-%Y'));

EOFEOF
}



#MAIN
BASE_DIR=/apps/DASHBOARD/REPORTS
CONFIG=${BASE_DIR}/config
BIN=${BASE_DIR}/bin
ALARM=${BASE_DIR}/alarm
LOG=${BASE_DIR}/logs
TEMP=${BASE_DIR}/temp
DATA=${BASE_DIR}/data
ouser=dash
opass=dash123
xlogin="dash/dash123"
xdb=elixirdb
OOKLA_NOTIF=/apps/DASHBOARD/temp/ookla_notify.txt


BODY=${BASE_DIR}/config/OOKLA_PS_body.txt
EMAIL_LIST=${BASE_DIR}/config/OOKLA_PS_list.txt
SENDER=elixir@globe.com.ph
HEADER=${BASE_DIR}/config/OOKLA_PS_header.cfg
TRAILER=${BASE_DIR}/config/OOKLA_PS_trailer.cfg
REPORT=${BASE_DIR}/REPORT_FILES/PS
BASE=/apps/DASHBOARD/alyl_glist
deyt=`date "+%Y%m%d"`
petsa=`petsa | grep DAYNO: | awk {'print $2'}`
SUBJECT=`echo "OOKLA SPEEDTEST PROFILING REPORT FOR ${petsa}"`
ATTACH=${REPORT}/SPEEDTEST_PROFILING_${petsa}_REG_DL.csv
ATTACH2=${REPORT}/SPEEDTEST_PROFILING_${petsa}_REG_UL.csv


cd ${BASE}

echo "`date` Processing..."

deyyt=`date "+%m/%d %H:%M"`
echo "${deyyt}Generating SPEEDTEST_PROFILING_${petsa}_REG_DL.csv" >> ${OOKLA_NOTIF}
notify

speed_prof_dl > ${BASE}/SPEEDTEST_PROFILING_${petsa}_REG_DL.csv

deyyt=`date "+%m/%d %H:%M"`
echo "${deyyt}Generating SPEEDTEST_PROFILING_${petsa}_REG_UL.csv" >> ${OOKLA_NOTIF}
notify

speed_prof_ul > ${BASE}/SPEEDTEST_PROFILING_${petsa}_REG_UL.csv

deyyt=`date "+%m/%d %H:%M"`
echo "${deyyt}Generating SPEEDTEST_PROFILING_${petsa}_BH_REG_DL.csv" >> ${OOKLA_NOTIF}
notify

speed_prof_dl_BH > ${BASE}/SPEEDTEST_PROFILING_${petsa}_BH_REG_DL.csv

deyyt=`date "+%m/%d %H:%M"`
echo "${deyyt}Generating SPEEDTEST_PROFILING_${petsa}_BH_REG_UL.csv" >> ${OOKLA_NOTIF}
notify

speed_prof_ul_BH > ${BASE}/SPEEDTEST_PROFILING_${petsa}_BH_REG_UL.csv


/apps/EMAIL/bin/email.ksh ${SENDER} ${EMAIL_LIST} ${BODY} "${SUBJECT}" ${ATTACH} ${ATTACH2}

echo "`date` Done..."

gzip -f SPEEDTEST_PROFILING_${petsa}_REG_DL.csv SPEEDTEST_PROFILING_${petsa}_REG_UL.csv ${REPORT}/SPEEDTEST_PROFILING_${petsa}_BH_REG_DL.csv ${REPORT}/SPEEDTEST_PROFILING_${petsa}_BH_REG_UL.csv


#END PROGRAM
