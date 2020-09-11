#!/bin/ksh

get_data()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(DOMAIN, ''),',',IFNULL(SEVERITY_TYPE, ''),',',IFNULL(CNT_IMPLEMENTED, ''),',',IFNULL(CNT_PARTIAL, ''),',',IFNULL(CNT_DEFERRED, '')
,',',IFNULL(CNT_REVERTED, ''),',',IFNULL(CNT_ABORTED, ''),',',IFNULL(TOTAL, ''),',',IFNULL(PSR, ''),',',IFNULL(ISR, ''))
from CM_PERF_PER_DOMAIN_SUMMARY
where extract_flag is NULL
order by 1;

EOFEOF
}


get_data1()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(DOMAIN, ''),',',IFNULL(SEVERITY_TYPE, ''),',',IFNULL(REGION, ''),',',IFNULL(CNT_IMPLEMENTED, ''),',',IFNULL(CNT_PARTIAL, ''),',',IFNULL(CNT_DEFERRED, '')
,',',IFNULL(CNT_REVERTED, ''),',',IFNULL(CNT_ABORTED, ''),',',IFNULL(TOTAL, ''),',',IFNULL(PSR, ''),',',IFNULL(ISR, ''))
from CM_PERF_PER_DOMAINREGION_SUMMARY
where extract_flag is NULL
order by 1;

EOFEOF
}


get_data2()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(DOMAIN, ''),',',IFNULL(SEVERITY_TYPE, ''),',',IFNULL(STATUS, ''),',',IFNULL(REGION, ''),',',IFNULL(TOTAL_MAJOR, ''),',',IFNULL(TOTAL_MINOR, ''),',',IFNULL(TOTAL_CRITICAL, '')
,',',IFNULL(TOTAL_PER_REASON, ''))
from CM_PERF_DEFERMENT_DOMAINREGION_SUMMARY
where extract_flag is NULL
order by 1;

EOFEOF
}


get_data3()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(REGION, ''),',',IFNULL(TOTAL, ''))
from CM_PERF_FORCE_MAJEUR_SUMMARY
where extract_flag is NULL
order by 1;

EOFEOF
}


get_data4()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(REGION, ''),',',IFNULL(CNT_IMPLEMENTED, ''),',',IFNULL(CNT_PARTIAL, ''),',',IFNULL(CNT_DEFERRED, '')
,',',IFNULL(CNT_REVERTED, ''),',',IFNULL(CNT_ABORTED, ''),',',IFNULL(TOTAL_ENDORSED, ''),',',IFNULL(PSR, ''),',',IFNULL(ISR, ''))
from CM_PERF_PER_REGION_SUMMARY
where extract_flag is NULL
order by 1;

EOFEOF
}



get_data5()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(DOMAIN, ''),',',IFNULL(REGION, ''),',',IFNULL(TOT_IMPLEMENTED, ''),',',IFNULL(TOT_PARTIAL, ''),',',IFNULL(TOT_DEFERRED, '')
,',',IFNULL(TOT_REVERTED, ''),',',IFNULL(TOT_ABORTED, ''),',',IFNULL(TOTAL, ''),',',IFNULL(PSR, ''),',',IFNULL(ISR, ''))
from CM_PERF_PER_DOMAINREGION_SUMMARY2
where extract_flag is NULL
order by 1;

EOFEOF
}



get_data6()
{
${MYSQL} KJOHN << EOFEOF

SELECT CONCAT(IFNULL(MONTH, ''),',',IFNULL(DOMAIN, ''),',',IFNULL(SEVERITY_TYPE, ''),',',IFNULL(REGION, ''),',',IFNULL(TOTAL_MAJOR, ''),',',IFNULL(TOTAL_MINOR, ''),',',IFNULL(TOTAL_CRITICAL, '')
,',',IFNULL(TOTAL, ''))
from CM_PERF_DEFERMENT_DOMAINREGION_SUMMARY2
where extract_flag is NULL
order by 1;

EOFEOF
}



update_flag()
{
${MYSQL} KJOHN << EOFEOF
	
UPDATE CM_PERF_PER_DOMAIN_SUMMARY SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_PER_DOMAINREGION_SUMMARY SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_DEFERMENT_DOMAINREGION_SUMMARY SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_PER_DOMAINREGION_SUMMARY2 SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_DEFERMENT_DOMAINREGION_SUMMARY2 SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_FORCE_MAJEUR_SUMMARY SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;
UPDATE CM_PERF_PER_REGION_SUMMARY SET EXTRACT_FLAG=1 where EXTRACT_FLAG is NULL;

EOFEOF
}



petsa=`date "+%Y%m%d%H%M%S"`
MYSQL=/apps/DASHBOARD/MYSQL/bin/mysqlmariadb
WORKDIR=/apps/DASHBOARD/alarms/glance
DASHBOARD=/apps/DASHBOARD/MYSQL/DASHBOARD


#CHECK IF DB IS UP
STATUS=`/apps/DASHBOARD/MYSQL/bin/check_db_status.sql`

if [ "${STATUS}" = "UP_AND_RUNNING" ]
then
echo "`date` MARIA DB is UP and RUNNING"
else
echo "`date` MARIA DB is DOWN...Exiting..."
exit 6
fi


/apps/DASHBOARD/bin/julie_test/bin/01_agg_CM_Performance.sh

get_data | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data.tmp
get_data1 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data1.tmp
get_data2 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data2.tmp
get_data3 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data3.tmp
get_data4 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data4.tmp
get_data5 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data5.tmp
get_data6 | grep , | sort -u > ${WORKDIR}/batch/cm_glance_data6.tmp


######## LOAD TO MYSQL DASHBOARD ###########
###data
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data.tmp
tail ${WORKDIR}/batch/cm_glance_data.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data.tmp
then

cp ${WORKDIR}/batch/cm_glance_data.tmp ${DASHBOARD}/input/cm_glance_data.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data.sql
mv ${DASHBOARD}/input/cm_glance_data.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi


###data1
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data1.tmp
tail ${WORKDIR}/batch/cm_glance_data1.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data1.tmp
then

cp ${WORKDIR}/batch/cm_glance_data1.tmp ${DASHBOARD}/input/cm_glance_data1.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data1.sql
mv ${DASHBOARD}/input/cm_glance_data1.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi



###data2
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data2.tmp
tail ${WORKDIR}/batch/cm_glance_data2.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data2.tmp
then

cp ${WORKDIR}/batch/cm_glance_data2.tmp ${DASHBOARD}/input/cm_glance_data2.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data2.sql
mv ${DASHBOARD}/input/cm_glance_data2.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi



###data3
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data3.tmp
tail ${WORKDIR}/batch/cm_glance_data3.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data3.tmp
then

cp ${WORKDIR}/batch/cm_glance_data3.tmp ${DASHBOARD}/input/cm_glance_data3.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data3.sql
mv ${DASHBOARD}/input/cm_glance_data3.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi



###data4
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data4.tmp
tail ${WORKDIR}/batch/cm_glance_data4.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data4.tmp
then

cp ${WORKDIR}/batch/cm_glance_data4.tmp ${DASHBOARD}/input/cm_glance_data4.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data4.sql
mv ${DASHBOARD}/input/cm_glance_data4.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi



###data5
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data5.tmp
tail ${WORKDIR}/batch/cm_glance_data5.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data5.tmp
then

cp ${WORKDIR}/batch/cm_glance_data5.tmp ${DASHBOARD}/input/cm_glance_data5.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data5.sql
mv ${DASHBOARD}/input/cm_glance_data5.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi




###data6
echo "DAILY FILE:"
head ${WORKDIR}/batch/cm_glance_data6.tmp
tail ${WORKDIR}/batch/cm_glance_data6.tmp
echo

if /usr/bin/test -s ${WORKDIR}/batch/cm_glance_data6.tmp
then

cp ${WORKDIR}/batch/cm_glance_data6.tmp ${DASHBOARD}/input/cm_glance_data6.txt

#LOAD TO DASHBOARD

echo "`date` Loading to Portal MySQL DB..."
/apps/DASHBOARD/MYSQL/bin/mysqlplus dashboard < /apps/DASHBOARD/MYSQL/bin/LOADERS/load_cm_glance_data6.sql
mv ${DASHBOARD}/input/cm_glance_data6.txt ${DASHBOARD}/input/done
else
echo "`date` No data Extracted..."
fi


mv ${WORKDIR}/batch/cm_glance_data.tmp ${WORKDIR}/batch/done/cm_glance_data${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data1.tmp ${WORKDIR}/batch/done/cm_glance_data1${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data2.tmp ${WORKDIR}/batch/done/cm_glance_data2${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data3.tmp ${WORKDIR}/batch/done/cm_glance_data3${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data4.tmp ${WORKDIR}/batch/done/cm_glance_data4${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data5.tmp ${WORKDIR}/batch/done/cm_glance_data5${petsa}.txt
mv ${WORKDIR}/batch/cm_glance_data6.tmp ${WORKDIR}/batch/done/cm_glance_data6${petsa}.txt
update_flag

#END PROGRAM
