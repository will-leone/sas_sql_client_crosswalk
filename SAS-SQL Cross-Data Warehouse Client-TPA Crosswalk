/****************************************
PURPOSE

Create lists of client-TPA's that
live only in the single-tenant SAS
data warehouse, only in the multi-
tenant SQL data warehouse, or in
both. Also, provide certain ad hoc
client-TPA lists.

****************************************/

OPTIONS THREADS SASTRACE = ',,,sa' SASTRACELOC = SASLOG NOSTSUFFIX;

/* turn off display of outputs/notes */
%MACRO ods_off;
    ODS EXCLUDE ALL;
    ODS NORESULTS;
    OPTIONS NONOTES;
%MEND;
 
/* re-enable display of outputs/notes */
%MACRO ods_on;
    ODS EXCLUDE NONE;
    ODS RESULTS;
    OPTIONS NOTES;
%MEND;

%ods_off

%LET user = wleone; 
LIBNAME myfiles "/sasprod/users/&user." ;
LIBNAME sasdata "/sasprod/ca/sasdata/Client_TPA_Availability";
LIBNAME lobfmt1 "/sasprod/dw/formats/mgmt";
LIBNAME lobfmt2 "/sasprod/dw/formats/portal";

/****************************************
SECTION 1

Create a persistent SAS dataset of SQL DW
client-TPA's.
****************************************/

/****************************************
1.1

Pull down the initial client-TPA-batch
lists

MAX(BatchID) represents the most recent
specific data load process used to
populate the record.
****************************************/
/* Establish the reading-optimized connection to EDW's PRD003 */
%INCLUDE "/prg/sasutil/sqlcode/sasuser_sql_creds.sas" ;

%LET prd003r_conn = %CMPRES(
    DSN %BQUOTE(=) "IPE1PR_PRDANALYTICS_R"
    USER %BQUOTE(=) "&sasusr"
    PASSWORD %BQUOTE(=) "&saspwd"
    TRACE %BQUOTE(=) YES
    TRACEFILE %BQUOTE(=) SASLOG
    READBUFF %BQUOTE(=) 10000
    INSERTBUFF %BQUOTE(=) 10000
        /* SAS reads/inserts/updates 250/10/1 record at a time by default */
        /* documentation: http://support.sas.com/resources/papers/proceedings13/081-2013.pdf */
    );

PROC SQL NOPRINT;
        CONNECT TO SQLSVR (&prd003r_conn.) ;
            CREATE TABLE _batches AS (
                SELECT * FROM CONNECTION TO SQLSVR (
                    SELECT
                        mef.CLIENT_ID
                        , c.CLIENT_NAME AS EDW_Client_Name
                        , c.CLIENT_CODE
                        , pd.TPA_ID
                        , t.TPA_NAME AS EDW_TPA_Name
                        , t.TPA_CODE
                        , MAX(mef.BATCH_ID) AS batchid
                    FROM EVH_DW..MEMBER_ENROLLMENT_FACT AS mef (NOLOCK)
                    INNER JOIN EVH_DW..PLAN_DIM AS pd (NOLOCK)
                        ON mef.PLAN_DIM_KEY = pd.PLAN_DIM_KEY
                    INNER JOIN EVH_DW..CLIENT AS c (NOLOCK)
                        ON mef.CLIENT_ID = c.CLIENT_ID
                    INNER JOIN EVH_DW..TPA AS t (NOLOCK)
                        ON pd.TPA_ID = t.TPA_ID
                    GROUP BY
                        mef.CLIENT_ID
                        , c.CLIENT_NAME
                        , c.CLIENT_CODE
                        , pd.TPA_ID
                        , t.TPA_NAME
                        , t.TPA_CODE
                    ;
    ));
QUIT;

/****************************************
1.2

Push the intial client-TPA-batch list
to SQL and pull down batch/source information.
****************************************/
%LET prd002_conn = %CMPRES(
    DSN %BQUOTE(=) "IPE1PR_DWDB_002_PRD_PRELOAD"
    USER %BQUOTE(=) "&sasusr"
    PASSWORD %BQUOTE(=) "&saspwd"
    TRACE %BQUOTE(=) YES
    TRACEFILE %BQUOTE(=) SASLOG
    READBUFF %BQUOTE(=) 10000
    INSERTBUFF %BQUOTE(=) 10000
        /* SAS reads/inserts/updates 250/10/1 record at a time by default */
        /* documentation: http://support.sas.com/resources/papers/proceedings13/081-2013.pdf */
    );

LIBNAME prd002 SQLSVR &prd002_conn. CONNECTION=GLOBAL;


DATA prd002.'##batches'n;  /* ## = global temporary table */
    SET _batches;
RUN;

/****************************************
1.3

Using the initial client-TPA-batch list,
pull down batch and source information.

BatchID represents the specific data load
process used to populate the record while
ParentBatchID indicates the data source
at the client-TPA-filetype level.

****************************************/
PROC SQL NOPRINT;
    CONNECT TO SQLSVR (&prd002_conn.) ;
        CREATE TABLE all_edw AS (
            SELECT * FROM CONNECTION TO SQLSVR (
                SELECT
                    CASE 
                        WHEN (btr.BatchTypeName LIKE '%PDS%'
                                OR bi.TPA_ID = 74)
                            THEN 'ETL 2.0'
                        ELSE 'ETL 1.0' 
                    END AS ETL_Pathway
                    , bi.EDW_Client_Name
                    , bi.CLIENT_CODE
                    , bi.CLIENT_ID
                    , bi.EDW_TPA_Name
                    , bi.TPA_CODE
                    , bi.TPA_ID
                FROM LOGGING..Batch_Log AS bl (NOLOCK)
                INNER JOIN LOGGING..Batch_Log AS bl2 (NOLOCK)
                    ON bl.ParentBatchID = bl2.batchid
                INNER JOIN LOGGING..BatchType_Ref AS btr (NOLOCK)
                    ON bl2.BatchTypeID = btr.BatchTypeID
                INNER JOIN ##batches AS bi (NOLOCK)
                   ON bl.BATCHID = bi.BATCHID;
    ));
QUIT;

PROC SQL NOPRINT;
    ALTER TABLE all_edw
    ADD Path CHAR(100)
        , PDW_Client_Name CHAR(20)
        , PDW_TPA_Name CHAR(20)
    ;
QUIT;

%MACRO format(dataset);
    PROC SQL NOPRINT;
        UPDATE &dataset.
        SET EDW_Client_Name = PROPCASE(EDW_Client_Name)
        WHERE LENGTH(EDW_Client_Name) > 4
        ;

        UPDATE &dataset.
        SET EDW_TPA_Name = PROPCASE(EDW_TPA_Name)
        WHERE LENGTH(EDW_TPA_Name) > 4
        ;
        
        /* Various redacted UPDATE statements with
           client-TPA names */
        
    QUIT;
%MEND;
%format(all_edw)

/* Determine Identifi Status */
PROC SQL NOPRINT;
        CONNECT TO SQLSVR (&prd003r_conn.) ;
            CREATE TABLE identifi AS (
                SELECT * FROM CONNECTION TO SQLSVR (
                    SELECT DISTINCT
                        mmf.CLIENT_ID
                        , mmf.TPA_ID
                    FROM EVH_DW..STRATIFICATION_FACT AS sf (NOLOCK)
                    INNER JOIN EVH_DW..MEMBER_MONTH_FACT AS mmf (NOLOCK)
                        ON mmf.MEMBER_DIM_KEY = sf.MEMBER_DIM_KEY
                    INNER JOIN EVH_DW..PLAN_DIM AS pd (NOLOCK)
                        ON pd.PLAN_DIM_KEY = mmf.PLAN_DIM_KEY
                            AND pd.PLAN_DIM_KEY = sf.PLAN_DIM_KEY
                            AND pd.Client_ID = mmf.Client_ID
                            AND pd.TPA_ID = mmf.TPA_ID
                    INNER JOIN EVH_DW..MEMBER_ENROLLMENT_FACT
                            AS mef (NOLOCK)
                        ON mef.PLAN_DIM_KEY = pd.PLAN_DIM_KEY
                            AND mef.MEMBER_DIM_KEY = mmf.MEMBER_DIM_KEY
                    ;
    ));
QUIT;

/* Update the all_edw dataset and make it persistent */
PROC SQL NOPRINT;
    CREATE TABLE myfiles.all_edw AS
        SELECT
            init.ETL_Pathway
            , CASE
                WHEN id.Client_ID IS NOT NULL
                    THEN 'Using'
                ELSE 'Not Using'
              END AS Identifi_Status
            , init.EDW_Client_Name
            , init.PDW_Client_Name
            , init.CLIENT_CODE
            , init.CLIENT_ID
            , init.EDW_TPA_Name
            , init.PDW_TPA_Name
            , init.TPA_CODE
            , init.TPA_ID
            , init.Path
        FROM all_edw AS init
        LEFT JOIN identifi AS id
            ON init.Client_ID = id.Client_ID
                AND init.TPA_ID = id.TPA_ID
        ;
QUIT;

/****************************************
SECTION 2

Create a table of all SAS DW client-TPA's
and build a crosswalk between matching
SAS/SQL DW client-TPA's.
****************************************/

/* Identify the SAS DW client-TPA's */
%MACRO pdw_linux;

    /* Running Linux commands inside a macro makes
       it easier to mask special characters in
       strings. */

    /* Linux code: locate all client-TPA
       directories containing Claims datasets. */
    %LET code_input_hps = "cd /sasprod%NRBQUOTE(;)find /sasprod \( -path /sasprod/shared -o -path /sasprod/users \) -prune -o -type f -wholename ""/sasprod/%NRBQUOTE(*)/sasdata/claims.sas7bdat"" -print";
    %LET code_input_nonhps = "cd /sasprod%NRBQUOTE(;) find /sasprod \( -path /sasprod/shared -o -path /sasprod/users \) -prune -o -type f -wholename ""/sasprod/%NRBQUOTE(*)/sasdata/%NRBQUOTE(*)/claims.sas7bdat"" -print";

    /* Execute Linux code and save to datasets */
    FILENAME hps PIPE &code_input_hps.;
    DATA myfiles.paths_hps;
       INFILE hps TRUNCOVER;
       LENGTH process $ 150;
       INPUT process $ CHAR150.;
    RUN;

    FILENAME nonhps PIPE &code_input_nonhps. ;
    DATA myfiles.paths_nonhps;
       INFILE nonhps TRUNCOVER;
       LENGTH process $ 150;
       INPUT process $ CHAR150.;
    RUN;

    /* Clear the filenames once done */
    FILENAME _ALL_ CLEAR;
%MEND;
%pdw_linux

PROC SQL;
    /* The above paths should be
       ignored if they contain
       the below substrings */
    CREATE TABLE ignore
        (substring VARCHAR(25))
    ;
    INSERT INTO ignore
    VALUES ('last')
    ;
    INSERT INTO ignore
    VALUES ('backup')
    ;
    INSERT INTO ignore
    VALUES ('restore')
    ;
    INSERT INTO ignore
    VALUES ('delete')
    ;
    INSERT INTO ignore
    VALUES ('temp')
    ;
    INSERT INTO ignore
    VALUES ('bkp')
    ;
    INSERT INTO ignore
    VALUES ('bkup')
    ;
    INSERT INTO ignore
    VALUES ('load')
    ;
    INSERT INTO ignore
    VALUES ('benchmark')
    ;
    INSERT INTO ignore
    VALUES ('innovation')
    ;
    INSERT INTO ignore
    VALUES ('risk scoring')
    ;
    INSERT INTO ignore
    VALUES ('new folder')
    ;
    INSERT INTO ignore
    VALUES ('rate_setting')
    ;
    INSERT INTO ignore
    VALUES ('rerun')
    ;
    INSERT INTO ignore
    VALUES ('archive')
    ;
    INSERT INTO ignore
    VALUES ('working')
    ;
    /* Remove the extra paths,
       including any combined dataset paths.
       Add client/TPA name fields. */
    CREATE TABLE paths_nonhps_cleaned AS
        SELECT
            p.process AS Path
            , SCAN(p.process, 2, '/') AS PDW_Client_Name
            , SCAN(p.process, 4, '/') AS PDW_TPA_Name
        FROM myfiles.paths_nonhps AS p 
        LEFT JOIN ignore as o
            ON (LOWCASE(p.process) LIKE CATS('%', o.substring,'%'))
        WHERE
            o.substring IS NULL
            AND ((SCAN(p.process, 2, '/')
                  <> SCAN(p.process, 4, '/'))
                 OR SCAN(p.process, 2, '/') = 'client_x')
        ;

    /* Merge the datasets. Add client/TPA
       fields for HPS dataset */
    CREATE TABLE myfiles.PDW_paths AS
        SELECT
            process AS Path
            , SCAN(process, 2, '/') AS PDW_Client_Name
            , 'client_a' AS PDW_TPA_Name
        FROM myfiles.paths_hps
        UNION ALL
        SELECT *
        FROM paths_nonhps_cleaned
    ;
    
    /* Redacted client-TPA specific UPDATE statements */
    
    UPDATE myfiles.PDW_paths
    SET Path = TRANWRD(Path, '/claims.sas7bdat', '')
    ;

QUIT;

/* Create the crosswalk between matching
   SAS/SQL DW client-TPA's using PDS'
   own crosswalk */
%INCLUDE "/prg/sasutil/sqlcode/sasuser_sql_creds.sas" ;
%LET caprod_conn= DSN="CA_ANALYTICS_PROD" USER="&sasusr" PASSWORD="&saspwd" ;
LIBNAME caprod SQLSVR &caprod_conn. CONNECTION=GLOBAL ;
PROC SQL NOPRINT;
    CONNECT TO SQLSVR (&caprod_conn.) ;
        CREATE TABLE gtl_xwalk_client AS (
            SELECT * FROM CONNECTION TO SQLSVR (
                SELECT
                    LOWER(from_value) AS PDW_Client_Name
                    , LOWER(from_desc) AS EDW_Client_Name
                    , to_value AS Client_ID
                    , to_desc AS Client_Code
                FROM ca_gtl.dbo.payerfiles_variablevalue
                WHERE group_name LIKE '%eh_client_code%'
        ));
        
        CREATE TABLE gtl_xwalk_tpa AS (
            SELECT * FROM CONNECTION TO SQLSVR (
                SELECT
                    SUBSTRING(LOWER(from_value), 4, LEN(from_value)) AS PDW_TPA_Name
                    , LOWER(from_desc) AS EDW_TPA_Name
                    , to_value AS TPA_ID
                    , to_desc AS TPA_Code
                FROM ca_gtl.dbo.payerfiles_variablevalue
                WHERE group_name LIKE '%eh_tpa_code%'
        ));
    
    
    /* Redacted UPDATEs to correct a handful of PDW TPA names */
    CREATE TABLE sasdata.all_PDW_and_EDW AS
        SELECT DISTINCT
            edw.ETL_Pathway
            , edw.Identifi_Status
            , edw.EDW_Client_Name
            , pdw.PDW_Client_Name
            , edw.Client_Code
            , edw.CLIENT_ID
            , edw.EDW_TPA_Name
            , pdw.PDW_TPA_Name
            , edw.TPA_Code
            , edw.TPA_ID
            , pdw.Path
        FROM myfiles.all_edw AS edw
        LEFT JOIN gtl_xwalk_client AS xc
            ON CATS(edw.Client_ID) = CATS(xc.Client_ID)
        LEFT JOIN gtl_xwalk_tpa AS xt
            ON CATS(edw.TPA_ID) = CATS(xt.TPA_ID)
        LEFT JOIN myfiles.PDW_paths AS pdw
            ON (pdw.Path = 'client_tpa_filepath'
                    AND edw.Client_ID = 31 AND edw.TPA_ID = 61)
                OR (LOWCASE(pdw.PDW_TPA_Name) = 'client_d'
                    AND edw.Client_ID = 29 AND edw.TPA_ID = 3)
                OR ((LOWCASE(pdw.PDW_Client_Name)
                        CONTAINS CATS(xc.PDW_Client_Name))
                    AND ((LOWCASE(pdw.PDW_TPA_Name) = 'tpa_b'
                          AND xt.TPA_ID = '20')
                        OR (LOWCASE(pdw.PDW_TPA_Name) = 'tpa_l'
                          AND xt.TPA_ID = '73')
                        OR (LOWCASE(pdw.PDW_TPA_Name) = 'tpa_c'
                          AND xt.TPA_ID = '10')
                        OR (LOWCASE(pdw.PDW_TPA_Name) CONTAINS CATS(xt.PDW_TPA_Name))
                        OR (LOWCASE(pdw.PDW_TPA_Name) CONTAINS LOWCASE(COMPRESS(edw.EDW_TPA_Name)))
                        OR (LOWCASE(xt.EDW_TPA_Name) CONTAINS LOWCASE(pdw.PDW_TPA_Name))
                        OR (LOWCASE(pdw.PDW_TPA_Name) = 'tpa_a'
                            AND (xt.TPA_ID IN ('7', '74', '86'))
                            )
                    ))
        ;

    UPDATE sasdata.all_pdw_and_edw
    SET
        ETL_Pathway = 'ETL 2.0 - NO CLAIMS DATA'
    WHERE ETL_Pathway = 'ETL 2.0'
        AND Path IS NULL
    ;
    
QUIT;

PROC SORT
    DATA = sasdata.all_pdw_and_edw;
    BY ETL_Pathway Identifi_Status EDW_CLIENT_NAME EDW_TPA_NAME;
RUN;

/****************************************
SECTION 4

Use the Client-TPA crosswalk between SAS DW
and SQL DW to create the remaining datasets.

****************************************/

PROC SQL NOPRINT;

    CREATE TABLE sasdata.all_edw AS
        SELECT *
        FROM sasdata.all_pdw_and_edw
        ;

    CREATE TABLE sasdata.all_pdw AS
        SELECT
            pe.ETL_Pathway
            , pe.Identifi_Status
            , p.PDW_Client_Name
            , pe.EDW_Client_Name
            , pe.Client_Code
            , pe.CLIENT_ID
            , p.PDW_TPA_Name
            , pe.EDW_TPA_Name
            , pe.TPA_Code
            , pe.TPA_ID
            , p.Path
        FROM myfiles.pdw_paths AS p
        LEFT JOIN sasdata.all_pdw_and_edw AS pe
            ON p.Path = pe.Path
        ;

    UPDATE sasdata.all_edw
    SET
        Path = 'N/A'
        , PDW_Client_Name = 'N/A'
        , PDW_TPA_Name = 'N/A'
    WHERE Path IS NULL
    ;

    UPDATE sasdata.all_pdw
    SET
        ETL_Pathway = 'ETL 1.0'
        , Identifi_Status = 'Not Using'
        , EDW_Client_Name = 'N/A'
        , Client_ID = -1
        , Client_Code = 'N/A'
        , EDW_TPA_Name = 'N/A'
        , TPA_ID = -1
        , TPA_Code = 'N/A'
    WHERE EDW_Client_Name IS NULL
    ;

    CREATE TABLE sasdata.edw_only AS
        SELECT * 
        FROM sasdata.all_edw
        WHERE Path = 'N/A'
        ;

    CREATE TABLE sasdata.pdw_only AS
        SELECT *
        FROM sasdata.all_pdw
        WHERE Client_ID = -1
        ;

    DELETE FROM sasdata.all_pdw_and_edw
    WHERE Path is NULL
    ;

QUIT;

PROC SORT
    DATA = sasdata.all_pdw;
    BY
        ETL_Pathway
        Identifi_Status
        EDW_CLIENT_NAME
        EDW_TPA_NAME
        PDW_CLIENT_NAME
        PDW_TPA_NAME
    ;
RUN;

PROC SORT
    DATA = sasdata.pdw_only;
    BY
        PDW_CLIENT_NAME
        PDW_TPA_NAME
    ;
RUN;

/* Create the Identifi-Using, Integrated CT list */
DATA sasdata.identifi_only
    (DROP = ETL_Pathway Identifi_Status);
    SET sasdata.all_pdw_and_edw;
    WHERE Identifi_Status = 'Using'
        AND ETL_Pathway = 'ETL 2.0';
RUN;

/*********************************
SECTION 5

Use the Client-TPA crosswalk between SAS DW
and SQL DW to create the remaining datasets.

*********************************/
%INCLUDE  /* pass user/password info*/ 
    "/prg/sasutil/sqlcode/sasuser_sql_creds.sas" ;
/* Establish reading-optimized connection to PRD003's EVH_DW */
%LET prd003 = DSN="IPE1PR_DWDB_003_PRD_EVH_DW_R" USER="&sasusr" PASSWORD="&saspwd" ;

/* PLAN_DIM client-TPA's */
PROC SQL NOPRINT;
    CONNECT TO SQLSVR(&prd003);
        CREATE TABLE myfiles.plan_dim AS 
            SELECT * FROM CONNECTION TO SQLSVR(
                SELECT DISTINCT
                    client_id
                    , tpa_id
                    , client_name
                    , tpa_name
                    , lob_category
                    , lob_desc
                    , lob_code
                FROM EVH_DW..PLAN_DIM
                ORDER BY
                    CLIENT_ID
                    , TPA_ID
                ;
            )
        ;

    /* Use EVHDW's PLAN_DIM to create a list
    of all client-TPA-LOB combos living in EDW */
    CREATE TABLE sasdata.all_edw_with_edw_lob AS (
        SELECT DISTINCT
            alledw.ETL_Pathway
            , alledw.Identifi_Status
            , alledw.PDW_Client_Name
            , alledw.EDW_Client_Name
            , alledw.Client_Code
            , alledw.CLIENT_ID
            , alledw.PDW_TPA_Name
            , alledw.EDW_TPA_Name
            , alledw.TPA_Code
            , alledw.TPA_ID
            , alledw.Path
            , pd.LOB_Code AS EDW_LOB_Code
            , CASE
                WHEN pd.lob_category = 'MEDICARE FFS'
                        AND NOT alledw.path CONTAINS 'ngaco'
                    THEN 'MSSP ACO'
                WHEN alledw.path CONTAINS 'ngaco'
                    THEN 'Next Generation ACO'
                ELSE PROPCASE(pd.lob_category)
              END AS EDW_LOB_Category
            , PROPCASE(pd.LOB_Desc) AS EDW_LOB_Description
        FROM sasdata.all_edw AS alledw
        LEFT JOIN myfiles.plan_dim AS pd
            ON pd.Client_ID = alledw.Client_ID
                AND pd.TPA_ID = alledw.TPA_ID
    );
QUIT;

/* Add an ID column to each output table
   to allow for easy looping in downstream
   code/reports. */
DATA sasdata.all_edw_with_edw_lob;
    SET sasdata.all_edw_with_edw_lob;
    ref_id = _N_;
RUN;
DATA sasdata.all_edw;
    SET sasdata.all_edw;
    ref_id = _N_;
RUN;
DATA sasdata.all_pdw;
    SET sasdata.all_pdw;
    ref_id = _N_;
RUN;
DATA sasdata.all_pdw_and_edw;
    SET sasdata.all_pdw_and_edw;
    ref_id = _N_;
RUN;
DATA sasdata.edw_only;
    SET sasdata.edw_only;
    ref_id = _N_;
RUN;
DATA sasdata.identifi_only;
    SET sasdata.identifi_only;
    ref_id = _N_;
RUN;
DATA sasdata.pdw_only;
    SET sasdata.pdw_only;
    ref_id = _N_;
RUN;

%MACRO pdw_lob;

    DATA myfiles.all_pdw
            (RENAME=(
                PDW_Client_Name=Client_Name
                PDW_TPA_Name=TPA_Name));
        SET sasdata.all_pdw;
    RUN;
    
    /* create a counter variable
    based on the number of client-tpa's
    in the dataset */
    PROC SQL NOPRINT;
        SELECT CATS(COUNT(*))
        INTO :count
        FROM myfiles.all_PDW
        ;
    QUIT;

    /* Set variables to be used when
       auto-creating libnames */
    PROC SQL;
        SELECT
            client_name
            , tpa_name
            , CATS('lref', ref_id)
            , path
        INTO
            :client_1-:client_&count.
            , :tpa_1-:tpa_&count.
            , :libref_1-:libref_&count.
            , :path_1-:path_&count.
        FROM myfiles.all_PDW
        ;
    QUIT;

    /* Iterate through every client-TPA directory in PDW */
    %DO i=1 %TO &count.;
        LIBNAME &&libref_&i. "&&Path_&i.." ACCESS = Read;

        /* Only execute if the core dataset exists */
        %IF %SYSFUNC(EXIST(&&libref_&i...eligibility))
        %THEN
            %DO;
                %PUT &&client_&i.. - &&tpa_&i..: Checking for LOB/LOB_EVH in Eligibility.;

                PROC CONTENTS DATA = &&libref_&i...eligibility 
                    MEMTYPE = DATA    
                    OUT = matrix_elig NOPRINT;
                RUN;

                PROC SQL NOPRINT;
                    SELECT CATS(COUNT(name))
                    INTO :vcount_lob
                    FROM matrix_elig
                    WHERE LOWCASE(name) = "lob"
                    ;

                    SELECT CATS(COUNT(name))
                    INTO :vcount_lobevh
                    FROM matrix_elig
                    WHERE LOWCASE(name) = "lob_evh"
                    ;
                QUIT;

                %IF %SYSFUNC(CATS(&vcount_lob.)) = %SYSFUNC(CATS(0))
                %THEN
                    %DO;
                        %PUT LOB is not in this dataset. ;
                    %END;
                %ELSE 
                    %DO;
                        %PUT Extracting LOB values.;
                        
                        PROC SQL NOPRINT;
                            CREATE TABLE myfiles.lob_lref&i. AS
                                SELECT DISTINCT
                                    "&&path_&i.." AS Path
                                    , "lob" AS Variable
                                    , lob AS Value
                                FROM &&libref_&i...eligibility
                            ;
                        QUIT;

                    %END;

                %IF %SYSFUNC(CATS(&vcount_lobevh.)) = %SYSFUNC(CATS(0))
                %THEN
                    %DO;
                        %PUT LOB_EVH is not in this dataset. ;
                    %END;
                %ELSE 
                    %DO;
                        %PUT Extracting LOB_EVH values.;
                        
                        PROC SQL NOPRINT;
                            CREATE TABLE myfiles.lobevh_lref&i. AS
                                SELECT DISTINCT
                                    "&&path_&i.." AS Path
                                    , "lob_evh" AS Variable
                                    , lob_evh AS Value
                                FROM &&libref_&i...eligibility
                            ;
                        QUIT;

                    %END;
            %END;
        %ELSE %PUT &&client_&i.. - &&tpa_&i..: Eligibility dataset is unavailable.;
    %END;

    DATA myfiles._final_pdw_lob_lookup;
        INFORMAT
            Path $100.
            Variable $32.
            Value $32.
            ;
        SET
            myfiles.lobevh_lref:
            myfiles.lob_lref:
            ;
        FORMAT
            Path $100.
            Variable $32.
            Value $32.
            ;
    RUN;
    
    /* Dedup cases where LOB/LOB_EVH take the same values */
    PROC SQL NOPRINT;
        CREATE TABLE myfiles.final_pdw_lob_lookup AS
        SELECT DISTINCT
            Path
            , Value
        FROM myfiles._final_pdw_lob_lookup
    QUIT;
    
    %IF %SYSFUNC(EXIST(myfiles.final_pdw_lob_lookup))
        %THEN
            %DO;

                PROC DATASETS LIBRARY=myfiles MEMTYPE=DATA NOLIST;
                    DELETE
                        lobevh_lref:
                        lob_lref:
                    ;
                RUN;

            %END;
        %ELSE;

    /* Create a crosswalk of client-TPA specific LOB's */
    DATA lob_xwalk
        (RENAME=(fmtname=PDW_Client_Name
                 label=PDW_LOB_Description
                 start=PDW_LOB_Code)
            DROP=type);
        INFORMAT
            type $32.
            start $32.
            label $100.
            fmtname $32.
            ;
        SET
            lobfmt1.lob_:
            lobfmt2.lob_:
            ;
        FORMAT
            type $32.
            start $32.
            label $100.
            fmtname $32.
            ;
        IF FIND(SUBSTR(fmtname, FIND(fmtname, "_") + 1), "_") = 0
        THEN fmtname = SUBSTR(fmtname, FIND(fmtname, "_") + 1);
        ELSE fmtname = SUBSTR(fmtname, FIND(fmtname, "_") + 1
            , FIND(SUBSTR(fmtname, FIND(fmtname, "_") + 1), "_") - 1)
        ;
    RUN;

    /* Create a list of all client-TPA-LOB combos
       living in PDW */
    PROC SQL NOPRINT;
        CREATE TABLE myfiles.lob_xwalk AS
            SELECT DISTINCT
                PDW_LOB_Description
                , PDW_LOB_Code
            FROM lob_xwalk
            ;
        
        CREATE TABLE myfiles.all_pdw_with_pdw_lob AS
            SELECT DISTINCT
                pdw.ETL_Pathway
                , pdw.Identifi_Status
                , pdw.PDW_Client_Name
                , pdw.EDW_Client_Name
                , pdw.Client_Code
                , pdw.CLIENT_ID
                , pdw.PDW_TPA_Name
                , pdw.EDW_TPA_Name
                , pdw.TPA_Code
                , pdw.TPA_ID
                , pdw.Path
                , CASE
                    WHEN pd.LOB_Code IS NOT NULL
                    THEN pd.LOB_Code
                    ELSE lob.value
                  END AS PDW_LOB_Code
                , CASE
                    WHEN pd.lob_category IS NOT NULL
                    THEN pd.LOB_category
                    WHEN x.PDW_LOB_Code IS NOT NULL
                    THEN "Client-TPA Specific"
                    ELSE "(Ask Client Analytics)"
                  END AS PDW_LOB_Category
                , CASE
                    WHEN pd.lob_desc IS NOT NULL
                    THEN PROPCASE(pd.lob_desc)
                    WHEN x.PDW_LOB_Code IS NOT NULL
                    THEN x.PDW_LOB_Description
                    ELSE "(Ask Client Analytics)"
                  END AS PDW_LOB_Description
                , CASE
                    WHEN pd.lob_category IS NOT NULL
                    THEN "Standard"
                    ELSE "Not Standard"
                  END AS Standard_LOB_Code
            FROM sasdata.all_PDW AS pdw
            LEFT JOIN myfiles.final_pdw_lob_lookup AS lob
                ON pdw.Path = lob.Path
            LEFT JOIN myfiles.plan_dim AS pd
                ON lob.value = pd.lob_code
            LEFT JOIN myfiles.lob_xwalk AS x
                ON x.PDW_LOB_Code = lob.value
            WHERE CALCULATED PDW_LOB_Code IS NOT NULL
            ;
    
        UPDATE myfiles.all_pdw_with_pdw_lob
        SET PDW_LOB_Category = 
            CASE
                WHEN PDW_LOB_Category = 'MEDICARE FFS'
                    AND NOT Path CONTAINS 'ngaco'
                THEN 'MSSP ACO'
                WHEN Path CONTAINS 'ngaco'
                THEN 'Next Generation ACO'
                ELSE PROPCASE(PDW_LOB_Category)
            END
        ;
        
        CREATE TABLE myfiles.lob_standard AS
            SELECT DISTINCT
                path
            FROM myfiles.all_pdw_with_pdw_lob
            WHERE standard_lob_code = "Standard"
            ;

        CREATE TABLE sasdata.all_pdw_with_pdw_lob AS
            SELECT
                m.ETL_Pathway
                , m.Identifi_Status
                , m.PDW_Client_Name
                , m.EDW_Client_Name
                , m.Client_Code
                , m.CLIENT_ID
                , m.PDW_TPA_Name
                , m.EDW_TPA_Name
                , m.TPA_Code
                , m.TPA_ID
                , m.Path
                , m.PDW_LOB_Code
                , m.PDW_LOB_Category
                , m.PDW_LOB_Description
            FROM myfiles.all_pdw_with_pdw_lob AS m
            LEFT JOIN myfiles.lob_standard AS lob
                ON m.path = lob.path
            WHERE (Standard_LOB_Code = "Standard"
                AND lob.path IS NOT NULL) OR
                lob.path IS NULL
            ;

    QUIT;

    DATA sasdata.all_pdw_with_pdw_lob;
        SET sasdata.all_pdw_with_pdw_lob;
        ref_id = _N_;
    RUN;

%MEND pdw_lob;
%pdw_lob

