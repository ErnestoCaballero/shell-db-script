#!/bin/ksh

# Author: Ernesto Caballero Delgado
# Created: 
# Last Modified:

# Description:
# Fills the CUSTOMER_PROD_* files generated after PROCESS
# that have missing data (product) for each Customer

# Usage: ./Missing_Product.sh <PROCESS_NO>

#############################################################################
# function  : _fetchRejectedCu
# parameters: none
# purpose   : Fetch the rejected CUST in the CL_R_rejects* files
#############################################################################
function _fetchRejectedCu {

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _fetchRejectedCu function" | tee -a ${LOG_FILE}

    BASE_PATH=/ercabde/prod/user/tmp

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Fetching the rejected CUST and TYPE from CL_R_rejects* files..." | tee -a ${LOG_FILE}

    for file in $(find ${BASE_PATH} -maxdepth 3 -type f -name "CL_R_rejects_$(date +%Y)${D_MONTH}00${PROCESS_CODE}*.csv"); do
        echo "Analyzing file ${file}" | tee -a ${LOG_FILE}
        cp ${file} ${BKP_DIR}
        grep -h 'Populate_product_id_AFI,INPUT_TEXT_P' ${file} | gawk -F "," '{print $3}' >> rejected_cust.txt
        grep -h 'Populate_product_id_AFI,INPUT_TEXT_P' ${file} | gawk -F "," '{print $7}' >> rejected_type.txt
    done

    # Format the files
    sed -i 's/ //g' rejected_cust.txt
    sed -i 's/ //g' rejected_type.txt

    less rejected_cust.txt | sort -n | uniq > rejected_cust_2.txt
    less rejected_type.txt | sort -n | uniq > rejected_type_2.txt

    rm rejected_cust.txt; mv rejected_cust_2.txt rejected_cust.txt
    rm rejected_type.txt; mv rejected_type_2.txt rejected_type.txt


    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating MISSING_PROD table holding rejected CUST" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND
        DROP TABLE MISSING_PROD;
        CREATE TABLE MISSING_PROD AS 
        SELECT cust_no from customer_table where 1=2;
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Inserting Rejected CUST into MISSING_PROD table..." | tee -a ${LOG_FILE}
    cat > ${CONTROL_FILE} << !
        LOAD DATA
        INFILE 'rejected_cust.txt'
        INTO TABLE MISSING_PROD
        FIELDS TERMINATED BY ',' optionally enclosed by '"'
        (cust_no)
!
    sqlldr ${CONDB1} control=${CONTROL_FILE}

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating MISSING_PROD_TYPE table holding rejected TYPES" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND
        DROP TABLE MISSING_PROD_TYPE;
        CREATE TABLE MISSING_PROD_TYPE AS 
        SELECT type_no FROM type_table WHERE 1=2;
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Inserting Rejected Subs into MISSING_PROD_TYPE table..." | tee -a ${LOG_FILE}
    cat > ${CONTROL_FILE} << !
        LOAD DATA
        INFILE 'rejected_type.txt'
        INTO TABLE MISSING_PROD_TYPE
        FIELDS TERMINATED BY ',' optionally enclosed by '"'
        (type_no)
!
    sqlldr ${CONDB1} control=${CONTROL_FILE}

    # Deleting tmp files for next execution
    rm rejected_cust.txt
    rm rejected_type.txt

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _fetchRejectedCu function" | tee -a ${LOG_FILE}
}

#############################################################################
# function  : _appendProMissing
# parameters: none
# purpose   : Fetch CUST with Missing Product in CUSTOMER_PROD_* files directly (Append CUST Missing)
#############################################################################
function _appendProMissing {

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _appendProMissing function" | tee -a ${LOG_FILE}
    
    BASE_PATH="/ercabde/prod/user/tmp/XLS*/"

    for file in $(find ${BASE_PATH} -maxdepth 4 -type f -name "CUSTOMER_PROD_${PROCESS_CODE}T$(date +%Y)${D_MONTH}${D_DAY}"); do
        # echo "copying file ${file} into directory ${BKP_DIR}" | tee -a ${LOG_FILE}
        # cp ${file} ${BKP_DIR}
        gawk -F "|" '$2 == "" {print $1}' $file >> miss_prod.txt
    done

    # Format the file
    sed -i 's/ //g' miss_prod.txt
    less miss_prod.txt | sort -n | uniq > miss_prod_2.txt
    rm miss_prod.txt; mv miss_prod_2.txt miss_prod.txt

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: File miss_prod.txt has been generated from CUSTOMER_PROD_* files" | tee -a ${LOG_FILE}

    # Create the table to hold the CUST with Missing Product
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND
        DROP TABLE tmp_MISSING_PROD_a;
        CREATE TABLE tmp_MISSING_PROD_a AS
        SELECT cust_no FROM customer_table WHERE 1=2;
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Inserting CUSTOMER into tmp_missing_prod_a table..." | tee -a ${LOG_FILE}
    cat > ${CONTROL_FILE} << !
        LOAD DATA
        INFILE 'miss_prod.txt'
        INTO TABLE tmp_missing_prod_a
        FIELDS TERMINATED BY ',' optionally enclosed by '"'
        (cust_no)
!
    sqlldr ${CONDB1} control=${CONTROL_FILE}

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating cust_no table tmp_missing_prod_b from CUSTOMER" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND

        DROP TABLE tmp_missing_prod_b;
        CREATE TABLE tmp_missing_prod_b AS
        SELECT DISTINCT ben cust_no FROM csm_ben a
            INNER JOIN tmp_MISSING_PROD_a b
                ON a.ban=b.cust_no;
        
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Join MISSING_PROD with tmp_MISSING_PROD_b to have all CUST" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND

        DROP TABLE tmp_union_tabs;
        CREATE TABLE tmp_union_tabs AS
        SELECT * FROM MISSING_PROD UNION SELECT * FROM tmp_MISSING_PROD_b;

        DROP TABLE MISSING_PROD;
        CREATE TABLE MISSING_PROD AS
        SELECT * FROM tmp_union_tabs;
        
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Join MISSING_PROD_TYPE with types in types_distr" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND

        DROP TABLE tmp_union_subs;
        CREATE TABLE tmp_union_subs AS
        SELECT DISTINCT customer_id
        FROM types_distr ch
            INNER JOIN tmp_MISSING_PROD_b tmp
                ON ch.target_pcn=tmp.cust_no
        WHERE 1=1
        AND expiration_date IS NULL
        UNION 
        SELECT * FROM MISSING_PROD_TYPE;

        DROP TABLE MISSING_PROD_TYPE;
        CREATE TABLE MISSING_PROD_TYPE AS
        SELECT * FROM tmp_union_subs;
        
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating bkp of miss_prod.txt into ${BKP_DIR}" | tee -a ${LOG_FILE}    
    cp miss_prod.txt miss_prod_${PROCESS_NO}.txt
    mv miss_prod_${PROCESS_NO}.txt ${BKP_DIR}

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _appendProMissing function" | tee -a ${LOG_FILE}

}


#############################################################################
# function  : _fetchProdDb2
# parameters: none
# purpose   : Fetch BA with Missing Plan in CUSTOMER_PROD_* files directly (Append FA Missing)
#############################################################################
function _fetchProdDb2 {

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _fetchProdDb2 function" | tee -a ${LOG_FILE}

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Fetching Offers from DB2 into MISSING_PROD_DB2 table" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB2} << !
        SPOOL ${LOG_FILE} APPEND

        DROP TABLE MISSING_PROD_TYPE;
        CREATE TABLE MISSING_PROD_TYPE AS
        SELECT * FROM pancustw.MISSING_PROD_TYPE@DB1;

        DROP TABLE tmp_inter_oms;
        CREATE TABLE tmp_inter_oms NOLOGGING PARALLEL 6 AS
        SELECT /+PARALLEL(T1, 6)/ T1.TYPE_ID,T1.PRODUCT_DEF,T1.PRODUCT_VER,T1.END_DATE, T1.ACTIVE AS EXP
        FROM PJL_ITEM T1
        LEFT OUTER JOIN PJL_ITEM T2
            ON (T1.TYPE_ID = T2.TYPE_ID AND T1.END_DATE < T2.END_DATE)
        WHERE T2.TYPE_ID IS NULL
        AND T1.TYPE_ID IN (SELECT TO_CHAR (cust_no) FROM MISSING_PROD_TYPE);

        DROP TABLE MISSING_PROD_DB2;
        CREATE TABLE MISSING_PROD_DB2 AS
        SELECT AP.TYPE_ID,CTI.CAption,AP.END_DATE 
        FROM tmp_inter_oms AP , PRODUCT_ITEM CTI
        WHERE AP.EXP = 1 AND AP.PRODUCT_DEF = CTI.CID
        AND AP.PRODUCT_VER = CTI.PCVERSION_ID
        AND CTI.ITEM_TYPE ='OF' ;
        
        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Recreating the table MISSING_PROD_DB2 in DB1" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} << !
        SPOOL ${LOG_FILE} APPEND

        DROP TABLE MISSING_PROD_DB2 ;
        CREATE TABLE MISSING_PROD_DB2 AS
        SELECT * FROM MISSING_PROD_DB2@DB2;

        DROP TABLE MISSING_PROD_NEW;
        CREATE TABLE MISSING_PROD_NEW as
        SELECT DISTINCT A.SERVICE_RECEIVER_ID,D.ACCOUNT_NO
        FROM ELEMENT_T A, CH_TYPE_TABLE B, PAY_TABLE D, MISSING_PROD C, MISSING_PROD_TYPE E
        WHERE B.PROCESS_NO = ${PROCESS_NO}
        AND B.D_KEY = ${D_KEY}
        AND B.cust_no = C.cust_no
        AND A.cust_no = B.cust_no
        AND A.D_KEY = B.CHARGE_D_KEY;

        SPOOL OFF
!

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating file with CUST having MISSING PRODUCT. File name: plan.txt" | tee -a ${LOG_FILE}
    sqlplus -s ${CONDB1} > plan_bkp_${TIME_STAMP}.txt << !
        SPOOL plan.txt

        set echo off
        set feedback off
        set verify off
        set head off
        set pages 0
        set termout off

        SELECT TO_CHAR(b.cust_no||'|'||trim(c.po_name))
        FROM MISSING_PROD_DB2 a , MISSING_PROD_NEW b , type_product c
        WHERE a.TYPE_ID = b.customer_id
        AND c.po_caption = a.caption
        AND a.caption is not null;

        SPOOL OFF
!

    mv plan_bkp_${TIME_STAMP}.txt ${BKP_DIR}

    # Formating plan.txt to remove empty spaces
    sed -i 's/ *$//g' plan.txt
    
    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _fetchProdDb2 function" | tee -a ${LOG_FILE}

}

#############################################################################
# function  : _missProdDb
# parameters: none
# purpose   : Create a file with CUST in CUSTOMER_PROD* files that don't have offers in DB2
#             if not found, look for valid offer in BE
#############################################################################
function _missProdDb {

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _missProdDb function" | tee -a ${LOG_FILE}

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Fetching CUST which don't have a matching plan in DB2 into miss.tmp" | tee -a ${LOG_FILE}
    while read cust; do
        b=$(grep "^${cust}" plan.txt)
        if [ $(echo ${b} | wc -c) -lt 2 ]; then
            echo ${cust} >> miss.tmp
            continue
        fi
    done < miss_prod.txt

    if [ -f miss.tmp ]; then
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: CUST with missing product were still found. Processing..." | tee -a ${LOG_FILE}
        # Create the table to hold the FA with Missing Plan in plan.txt
        sqlplus -s ${CONDB1} << !
            SPOOL ${LOG_FILE} APPEND
            DROP TABLE tmp_cust_MISSING_PROD;
            CREATE TABLE tmp_cust_MISSING_PROD AS
            SELECT cust_no FROM customer_table WHERE 1=2;
            SPOOL OFF
!

        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Inserting CUST into tmp_cust_MISSING_PROD table..." | tee -a ${LOG_FILE}
        cat > ${CONTROL_FILE} << !
            LOAD DATA
            INFILE 'miss.tmp'
            INTO TABLE tmp_cust_missing_prod
            FIELDS TERMINATED BY ',' optionally enclosed by '"'
            (cust_no)
!
        sqlldr ${CONDB1} control=${CONTROL_FILE}
        
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Searching for a valid Plan in Backend..." | tee -a ${LOG_FILE}
        sqlplus -s ${CONDB1} << !
            SPOOL ${LOG_FILE} APPEND

            DROP TABLE tmp_be_plan_valid;
            CREATE TABLE tmp_be_plan_valid AS
            SELECT DISTINCT cust, DESCRIPTION as caption 
            FROM CHANNEL_TYPE A , ELEMENT_T B, CH_TYPE_TABLE C,ELEMENT_T_CODE E  
            WHERE BAN IN (SELECT * FROM tmp_cust_missing_prod)
            AND  C.cust_no = A.type_cust
            AND C.D_KEY = ${D_KEY}
            AND C.PROCESS_NO = ${PROCESS_NO}
            AND B.cust_no = C.cust_no;

            SPOOL OFF
!

        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Generating file with Product found for each CUST..." | tee -a ${LOG_FILE}
        sqlplus -s ${CONDB1} >> ${LOG_FILE} << !
            SPOOL plan1.txt 

            set echo off
            set feedback off
            set verify off
            set head off
            set pages 0
            set termout off

            SELECT TO_CHAR(cust || '|' || caption) cust_prod 
            FROM tmp_be_plan_valid;

            SPOOL OFF
!

        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating file with pending FA to match with a Plan" | tee -a ${LOG_FILE}
        sqlplus -s ${CONDB1} >> ${LOG_FILE} << !
            SPOOL cust_notfound.txt 

            set echo off
            set feedback off
            set verify off
            set head off
            set pages 0
            set termout off

            SELECT TO_CHAR(cust_no) fa_no FROM pancustw.tmp_cust_missing_prod 
            MINUS SELECT TO_CHAR(cust_id) FROM tmp_be_plan_valid;

            SPOOL OFF
!

    else
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: All missing CUST found a matching plan in DB2." | tee -a ${LOG_FILE}
    fi

    if [ $(cat cust_notfound.txt | wc -c) -lt 1 ]; then
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: A valid plan was found for every CUST in CUSTOMER_PROD file" | tee -a ${LOG_FILE}
        rm cust_notfound.txt
    else
        # Implelment mail notification
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: There are still pending CUST to find a valid PRODUCT. " | tee -a ${LOG_FILE}
        for cust in $(cat cust_notfound.txt); do
            echo ${cust} >> ${LOG_FILE}
        done
    fi

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _missProdDb function" | tee -a ${LOG_FILE}

}


#############################################################################
# function  : _createMasterFile
# parameters: none
# purpose   : Create file holding all CUST Product combinations found
#############################################################################
function _createMasterFile {
    
    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _createMasterFile function" | tee -a ${LOG_FILE}
    
    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Creating master file with all CUST Product combination found" | tee -a ${LOG_FILE}

    if [ ! -f plan1.txt ]; then
        touch plan1.txt
    fi

    cp plan.txt inter_master.txt
    cat plan1.txt >> inter_master.txt

    sed "/^$/d" inter_master.txt | sed "s/ *$//g" | sort -n | uniq > master_file.txt

    rm inter_master.txt

    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _createMasterFile function" | tee -a ${LOG_FILE}

}


#############################################################################
# function  : _fixCustProd
# parameters: none
# purpose   : Fixing failed files and creating bkps
#############################################################################
function _fixCustProd {
    
    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Enter _fixCustProd function" | tee -a ${LOG_FILE}

    BASE_PATH="/ercabde/prod/user/tmp/XLS*/"

    count=0

    for file in $(find ${BASE_PATH} -maxdepth 2 -type f -name "CUSTOMER_PROD_${PROCESS_CODE}T$(date +%Y)${D_MONTH}${D_DAY}"); do

        count=$((count + 1))

        file_name=$(echo ${file} | gawk -F "/" '{print $NF}')
        WEP=$(echo ${file} | gawk -F "/" '{print $(NF - 1)}')

        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Analyzing file BASE_PATH/${WEP}/${file_name} number ${count}" | tee -a ${LOG_FILE}

        rm pending_cust_${WEP}_${file_name}.txt

        gawk -F "|" '$2=="" {print $1}' ${file} > curr_miss_prod_${count}.txt

        cust_miss_count=$(cat curr_miss_prod_${count}.txt | wc -l)
        
        echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tThe count of CUST Missing Product is ${cust_miss_count}" | tee -a ${LOG_FILE}

        if [ ${cust_miss_count} -ne 0 ]; then
            echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tStart fixing file BASE_PATH/${WEP}/${file_name}" | tee -a ${LOG_FILE}
            
            echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tCreating BKP: cp ${file} ${BKP_DIR}/${WEP}/${file_name}.${WEP}.bkp" | tee -a ${LOG_FILE}

            cp ${file} ${BKP_DIR}/${WEP}/${file_name}.${WEP}.bkp

            cp ${file} ${file}.inter

            INTER=${file}.inter

            for cust in $(cat curr_miss_prod_${count}.txt); do
                b=$(grep "^${cust}" master_file.txt)
                if [ "${b}" != "" ]; then
                    sed -i "/${cust}/d" ${INTER}
                    grep ${cust} master_file.txt >> ${INTER}
                    echo "${cust} was found in master_file.txt and processed" | tee -a ${LOG_FILE}
                    # echo "" > /dev/null
                else
                    echo "${cust} was not found in master_file.txt and processed" >> pending_cust_${WEP}_${file_name}.txt
                    # echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tThe ${fa} in file BASE_PATH/${WEP}/${file_name} had no matching plan in neither OMS nor BE. Manually check it." | tee -a ${LOG_FILE}
                fi
            done

            sort -n ${INTER} > ${file}

            rm ${INTER}

            echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tEnd fixing file BASE_PATH/${WEP}/${file_name}" | tee -a ${LOG_FILE}

        else
            echo "[$(date +'%d-%m-%Y %H:%M:%S')]: \tFile BASE_PATH/${WEP}/${file_name} had no Missing Product entries." | tee -a ${LOG_FILE}
            rm curr_miss_prod_${count}.txt
            continue
        fi

    done


    echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Exit _fixCustProd function" | tee -a ${LOG_FILE}

}


#############################################################################
# function  : main
# parameters: none
# purpose   : Setup globals, call functions.
#############################################################################

# PARAMETERS
PROCESS_NO=$1

if [ "$#" -ne 1 ]; then
    echo PARAM PASSED $*
    echo "Usage: $0 <PROCESS_NO>"
    exit 0
fi

# Paths:
typeset -r TIME_STAMP=$(date +%Y%m%d%H%M%S)
typeset -r LOG_FILE=/ercabde/MISSING_PROD/LOGS/MISSING_PROD_${PROCESS_NO}_${TIME_STAMP}.log
typeset -r BKP_DIR=/ercabde/MISSING_PROD/LOGS/BKP_CUST_PROD_${PROCESS_NO}_${TIME_STAMP}

# Initial Files Creation:
touch ${LOG_FILE}
mkdir ${BKP_DIR}
mkdir ${BKP_DIR}/WLH
mkdir ${BKP_DIR}/WLH_ENTERPRISE

# DB variables:
CONDB1=$AP  
CONDB2="OPS/OPS123@HOST"
CONTROL_FILE="control_file.ctl"

PARAMTRS=$(sqlplus -s ${CONDB1} << !
    SET SERVEROUTPUT ON SIZE 1000000
    SET HEADING OFF
    SET ECHO OFF
    SET TERM OFF
    SET TRIMSPOOL ON
    SET FEEDBACK OFF
    SET AUTOTRACE OFF
    SET PAGESIZE 0
    SET LINESIZE 200
    SET VERIFY OFF

    SELECT TO_CHAR(SUBSTR(MAX(rq.sys_creation_date), 1, 2)) day, 
        TO_CHAR(ct.instance) month, 
        TO_CHAR(ct.PROCESS_CODE) PROCESS_CODE,
        TO_CHAR(ct.D_KEY) D_KEY
    FROM table_a rq
        INNER JOIN table_b ct
            ON rq.process_id=ct.PROCESS_NO
    WHERE 1=1 
    AND PROCESS_NO=${PROCESS_NO}
    GROUP BY ct.instance,ct.PROCESS_CODE, ct.D_KEY;
!
)

echo $PARAMTRS

typeset -Z2 D_DAY=$(echo $PARAMTRS | awk '{print $1}')
typeset -Z2 D_MONTH=$(echo $PARAMTRS | awk '{print $2}')
typeset -Z2 PROCESS_CODE=$(echo $PARAMTRS | awk '{print $3}')
typeset -Z2 D_KEY=$(echo $PARAMTRS | awk '{print $4}')

echo "[$(date +'%d-%m-%Y %H:%M:%S')]: Initial Parameters:\nPROCESS_NO: ${PROCESS_NO}\nPROCESS_CODE: ${PROCESS_CODE}\nD_KEY: ${D_KEY}\nD_DAY: ${D_DAY}\nD_MONTH: ${D_MONTH}" | tee -a ${LOG_FILE}

# Fetch population with missing plan from reject files of type add.CL_R_rejects*
_fetchRejectedCu

# Fetch BA with Missing Plan in CUSTOMER_PROD_* files directly (Append CUST Missing)
_appendProMissing

# Fetch missing offers from DB2
_fetchProdDb2

# Create file with CUST which don't have PLAN in DB2
_missProdDb

# Create master file with all CUST Product found
_createMasterFile

# Fix CUSTOMER_PROD files
_fixCustProd


# rm miss_prod.txt

exit 0