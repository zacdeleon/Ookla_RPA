#!/usr/bin/ksh

FIRSTLOAD_TMP()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE MCS_LTE_AllCell_KPIs_TMP as select * from MCS_LTE_AllCell_KPIs where 1=0;
LOAD DATA
LOCAL INFILE '/apps/DASHBOARD/alarms/mcs/batch/MCS_LTE_AllCell_KPIs.load'
INTO TABLE MCS_LTE_AllCell_KPIs_TMP
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

EOFEOF
}

COUNTOFTMP()
{
${MYSQL} KJOHN << EOFEOF

SELECT COUNT(*) FROM MCS_LTE_AllCell_KPIs_TMP;

EOFEOF
}

DONESUCC_DELTMP()
{
${MYSQL} KJOHN << EOFEOF

DROP TABLE MCS_LTE_AllCell_KPIs_TMP;

EOFEOF
}


#MAIN
INP_DIR=/apps/DASHBOARD/MYCOM/input
INP_DONE=${INP_DIR}/done
petsa=`date "+%Y%m%d%H%M%S"`
BASE_DIR=/apps/DASHBOARD
BIN=${BASE_DIR}/bin
CONTROLFILE=${BIN}/controlfile
LOG=${BASE_DIR}/logs
LOADER=${BASE_DIR}/alarms/mcs/batch
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

#CHECK DATA
xflag=`ls -lrt ${INP_DIR}/LTE_AllCell_KPIs_DailyBP-*.csv | wc -l | awk {'print $1'}` 2> /dev/null

> ${LOADER}/MCS_LTE_AllCell_KPIs.load
if [ ${xflag} -gt 0 ]
then
cat ${INP_DIR}/LTE_AllCell_KPIs_DailyBP-*.csv | dos2unix | grep -v "Short name" | sed -e 's/N\/A//g' >> ${LOADER}/MCS_LTE_AllCell_KPIs.load
mv ${INP_DIR}/LTE_AllCell_KPIs_DailyBP-*.csv ${INP_DONE}
gzip -f ${INP_DONE}/LTE_AllCell_KPIs_DailyBP-*.csv


#LOAD TO DATABASE
FIRSTLOAD_TMP

tmpverification=`COUNTOFTMP | grep -v count`
ccount=`cat /apps/DASHBOARD/alarms/mcs/batch/MCS_LTE_AllCell_KPIs.load | wc -l | sed 's/ //g'`

if [ $tmpverification = $ccount ]
then
${MYSQL} KJOHN < ${CONTROLFILE}/01_MCS_LTE_AllCell_KPIs.sql
echo "Table count: [ $tmpverification ] = Load count: [ $ccount ] "
echo "MATCHED!"
echo "Loading Successful...   ROWS LOADED: ${tmpverification}"
else
${MYSQL} KJOHN < ${CONTROLFILE}/01_MCS_LTE_AllCell_KPIs.sql
echo "Table count: [ $tmpverification ] != Load count: [ $ccount ] "
echo "Does not matched."
echo "Loading unsuccessful...   ROWS NOT LOADED. COUNT OF TEMP TABLE: [ $tmpverification ]"
fi

else
echo "`date` No Data Found...Exiting..."
fi

DONESUCC_DELTMP
rm ${LOADER}/MCS_LTE_AllCell_KPIs.load


#END PROGRAM
