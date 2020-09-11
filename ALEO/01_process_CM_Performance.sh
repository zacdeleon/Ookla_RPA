#!/usr/bin/ksh

FIRSTLOAD_TMP()
{
${MYSQL} KJOHN << EOFEOF

CREATE TABLE CM_PERFORMANCE_INITIAL_TMP_Y as select * from CM_PERFORMANCE_INITIAL where 1=0;
load data local infile '/apps/DASHBOARD/bin/julie_test/loader/CM_Loader.load'
into table CM_PERFORMANCE_INITIAL_TMP_Y
fields terminated by ','
optionally enclosed by '"'
LINES TERMINATED BY '\n'

EOFEOF
}

COUNTOFTMP()
{
${MYSQL} KJOHN << EOFEOF

select count(*) from CM_PERFORMANCE_INITIAL_TMP_Y;

EOFEOF
}

DONESUCC_DELTMP()
{
${MYSQL} KJOHN << EOFEOF

DROP TABLE CM_PERFORMANCE_INITIAL_TMP_Y;
commit;

EOFEOF
}


petsa=`date "+%Y%m%d%H%M%S"`
BASE_DIR=/apps/DASHBOARD/bin/julie_test
CM_INPUT=${BASE_DIR}/input
CM_INPUT_DONE=${CM_INPUT}/done
LOADER=${BASE_DIR}/loader
MYSQL=/apps/DASHBOARD/MYSQL/bin/mysqlmariadb


#CHECK IF DB IS UP
STATUS=`/apps/DASHBOARD/MYSQL/bin/check_db_status.sql`

if [ "${STATUS}" = "UP_AND_RUNNING" ]
then
echo "`date` ELIXIR DB is UP and RUNNING"
else
echo "`date` ELIXIR DB is DOWN...Exiting..."
#rm $LOCKFILE
exit 6
fi

#CHECK DATA
xflag=`ls -lrt ${CM_INPUT}/CM_Raw_Data*.csv | wc -l | awk {'print $1'}` 2> /dev/null

> ${LOADER}/CM_Loader.load

if [ ${xflag} -gt 0 ]
then
echo "`date` Raw Data Found."
cat ${CM_INPUT}/CM_Raw_Data*.csv | dos2unix | sed '1d' | gawk -v RS='"' 'NR % 2 == 0 { gsub(/\n/, ",") } { printf("%s%s", $0, RT) }' | cut -d"," -f2- | sed -e 's/N\/A//g' >> ${LOADER}/CM_Loader.load
mv -f ${CM_INPUT}/CM_Raw_Data*.csv ${CM_INPUT_DONE}

#LOAD TO DATABASE
FIRSTLOAD_TMP

tmpverification=`COUNTOFTMP | grep -v count`
ccount=`cat /apps/DASHBOARD/bin/julie_test/loader/CM_Loader.load | wc -l | sed 's/ //g'`

if [ $tmpverification = $ccount ]
then
${MYSQL} KJOHN < ${BASE_DIR}/controlfile/01_cm_perf.sql
echo "Table count: [ $tmpverification ] = Load count: [ $ccount ] "
echo "MATCHED!"
echo "Loading Successful...   ROWS LOADED: ${tmpverification}"
DONESUCC_DELTMP
else
echo "Table count: [ $tmpverification ] != Load count: [ $ccount ] "
echo "Does not matched."
echo "Loading unsuccessful...   ROWS LOADED: ${tmpverification}"
DONESUCC_DELTMP
fi

else
echo "`date` No Data Found...Exiting..."
fi

#END PROGRAM
