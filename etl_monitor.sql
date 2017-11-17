CREATE OR REPLACE PACKAGE         etl_job_monitor_pkg AS

-------------------- job ------------------------------------
  PROCEDURE sf_add_job(p_job_name_in           VARCHAR2
                      ,p_job_description_in    VARCHAR2
                      ,p_starting_procedure_in VARCHAR2);
-------------------- job monitoring -------------------------
  FUNCTION sf_start_job_monitoring(p_job_id_in NUMBER) RETURN NUMBER;
  FUNCTION sf_start_job_monitoring(p_job_id_in         NUMBER
                                  ,p_reporting_date_in DATE
                                  ,p_debug_mode_in     NUMBER DEFAULT 0)
    RETURN NUMBER;
  PROCEDURE sp_end_job_monitoring(p_job_monitor_id NUMBER);  
  PROCEDURE sp_report_job_error(p_job_monitor_id     NUMBER
                               ,p_c_error_message_in VARCHAR2);
                               
                               
  /**********************************************************************************************
  * sf_is_job_successfully_ended - funkcja sprawdza czy ostatnie uruchomienie joba              *
  *                                zakończyło się prawidołowo                                   *
  *                  p_job_id_in => identyfikoator joba z tabeli ETL_JOB                        *
  * zwraca wartości: 1 - jeżeli job zakończony prowidłowo                                       *
  *                  0 - jeżeli job jest w statusie innym niż SUCCESSFULLY_ENDED                *
  *                 -1 - jeżeli job jest w statusie ERROR                                       *
  *                -10 - jeżeli wystąpił wewnętrzny bład                                        * 
  ***********************************************************************************************/ 
  FUNCTION sf_is_job_successfully_ended(p_job_id_in NUMBER) RETURN NUMBER;
  
  FUNCTION get_job_monitor_id(p_job_id_in NUMBER) RETURN NUMBER;
-------------------- end job --------------------------------

-------------------- job stage monitoring -------------------
  PROCEDURE sp_start_stage_monitoring(p_job_monitor_id    NUMBER
                                     ,p_stage_name_in     VARCHAR2
                                     ,p_key_table_name_in VARCHAR2);
  PROCEDURE sp_end_stage_monitoring(p_job_monitor_id             NUMBER
                                   ,p_key_table_name_in          VARCHAR2
                                   ,p_report_date_column_name_in VARCHAR2 DEFAULT NULL);  
  PROCEDURE sp_report_stage_error(p_job_monitor_id     NUMBER
                                 ,p_c_error_message_in VARCHAR2);

  
  /***********************************************************************************************
  * sf_is_stage_successfully_ended - funkcja sprawdza czy ostatni etap w wskazanym jobie         *
  *                                  zakończył się prawidołowo                                   *
  *                    p_job_id_in => identyfikoator joba z tabeli ETL_JOB                       *
  *                p_stage_name_in => nazwa etapu dla której chcemy sprawdzić status             *
  * zwraca wartości: 1 - jeżeli etap zakończony prowidłowo                                       *
  *                  0 - jeżeli etap jest w statusie innym niż SUCCESSFULLY_ENDED                *
  *                 -1 - jeżeli etap jest w statusie ERROR                                       *
  *                -10 - jeżeli wystąpił wewnętrzny bład                                         *  
  ***********************************************************************************************/ 
  FUNCTION sf_is_stage_successfully_ended(p_job_id_in         NUMBER
                                         ,p_stage_name_in     VARCHAR2) RETURN NUMBER;
-------------------- end job stage monitoring ----------------

  
  -- procedury nieaktualne --------------
/*  PROCEDURE set_debug_mode_on;
  PROCEDURE set_debug_mode_off;*/

END etl_job_monitor_pkg;
/

CREATE OR REPLACE PACKAGE BODY         CRM_ZAP.ETL_JOB_MONITOR_PKG AS

  g_debug_mode_status PLS_INTEGER := 0;
  g_i_is_error_occured  PLS_INTEGER := 0;

  FUNCTION get_last_job_stage_id(p_job_monitor_id NUMBER) RETURN NUMBER;
  FUNCTION get_job_id(p_job_monitor_id NUMBER) RETURN NUMBER;
  FUNCTION get_job_name(p_job_id_in NUMBER) RETURN VARCHAR2;
  FUNCTION get_table_rowcount(p_table_name_in              VARCHAR2
                             ,p_report_date_column_name_in VARCHAR2 DEFAULT NULL)
    RETURN NUMBER;
  PROCEDURE set_job_counters(p_job_id_in                NUMBER
                            ,p_is_properly_completed_in NUMBER);

  FUNCTION sf_start_job_monitoring(p_job_id_in NUMBER) RETURN NUMBER IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_i_job_monitor_id NUMBER(20);
    l_c_start_user     VARCHAR2(50);
    l_c_job_name       VARCHAR2(30);
  BEGIN
    g_i_is_error_occured := 0;
    l_i_job_monitor_id := to_number(to_char(SYSDATE, 'YYMMDDHH24MISS') ||
                                    to_char(p_job_id_in));
    l_c_start_user     := upper(sys_context('USERENV', 'OS_USER'));
    l_c_job_name       := get_job_name(p_job_id_in);
  
    INSERT INTO etl_job_monitor
      (job_monitor_id
      ,job_id
      ,job_name
      ,job_status
      ,operating_time_job
      ,start_time_job
      ,end_time_job
      ,start_time_current_stage
      ,current_stage_name
      ,start_user
      ,reporting_date)
    VALUES
      (l_i_job_monitor_id
      ,p_job_id_in
      ,l_c_job_name
      ,'RUNNING'
      ,NULL
      ,SYSDATE
      ,NULL
      ,SYSDATE
      ,'start'
      ,l_c_start_user
      ,trunc(SYSDATE, 'DD'));
    COMMIT;
  
    RETURN l_i_job_monitor_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END sf_start_job_monitoring;

  FUNCTION sf_start_job_monitoring(p_job_id_in         NUMBER
                                  ,p_reporting_date_in DATE
                                  ,p_debug_mode_in     NUMBER DEFAULT 0)
    RETURN NUMBER IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_i_job_monitor_id NUMBER(20);
    l_c_start_user     VARCHAR2(50);
    l_c_job_name       VARCHAR2(30);
  BEGIN
  
    g_debug_mode_status := p_debug_mode_in;
    l_i_job_monitor_id  := to_number(to_char(SYSDATE, 'YYMMDDHH24MISS') ||
                                     to_char(p_job_id_in));
    l_c_start_user      := upper(sys_context('USERENV', 'OS_USER'));
    l_c_job_name        := get_job_name(p_job_id_in);
  
    INSERT INTO etl_job_monitor
      (job_monitor_id
      ,job_id
      ,job_name
      ,job_status
      ,operating_time_job
      ,start_time_job
      ,end_time_job
      ,start_time_current_stage
      ,current_stage_name
      ,start_user
      ,reporting_date)
    VALUES
      (l_i_job_monitor_id
      ,p_job_id_in
      ,l_c_job_name
      ,'RUNNING'
      ,NULL
      ,SYSDATE
      ,NULL
      ,SYSDATE
      ,'start'
      ,l_c_start_user
      ,p_reporting_date_in);
    COMMIT;
  
    RETURN l_i_job_monitor_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END sf_start_job_monitoring;

  PROCEDURE sf_add_job(p_job_name_in           VARCHAR2
                      ,p_job_description_in    VARCHAR2
                      ,p_starting_procedure_in VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO etl_job
      (job_id
      ,job_name
      ,description
      ,creat_date
      ,mod_date
      ,is_active
      ,server
      ,SCHEMA
      ,starting_procedure
      ,run_counter
      ,error_counter)
    VALUES
      (etl_job_seq.NEXTVAL
      ,upper(p_job_name_in)
      ,p_job_description_in
      ,SYSDATE
      ,SYSDATE
      ,1
      ,sys_context('userenv', 'DB_NAME')
      ,sys_context('userenv', 'CURRENT_USER')
      ,p_starting_procedure_in
      ,0
      ,0);
    COMMIT;
    dbms_output.put_line('Given ID for ' || upper(p_job_name_in) || ': '|| to_char(etl_job_seq.CURRVAL));
  END;

  PROCEDURE sp_start_stage_monitoring(p_job_monitor_id    NUMBER
                                     ,p_stage_name_in     VARCHAR2
                                     ,p_key_table_name_in VARCHAR2) IS
    l_c_job_name VARCHAR2(30);
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    l_c_job_name := upper(get_job_name(get_job_id(p_job_monitor_id)));
  
    UPDATE etl_job_monitor
       SET start_time_current_stage = SYSDATE
          ,operating_time_job       = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                                      to_dsinterval(to_char(start_time_job, 'DD HH24:MI:SS'))
          ,current_stage_name       = upper(p_stage_name_in)
     WHERE etl_job_monitor.job_monitor_id = p_job_monitor_id;
  
    INSERT INTO etl_job_stage_monitor
      (job_stage_id
      ,job_monitor_id
      ,job_name
      ,stage_name
      ,stage_status
      ,operating_time
      ,start_time
      ,end_time
      ,key_table_rowcount
      ,key_table_name
      ,error_report)
      SELECT etl_job_stage_monitor_seq.NEXTVAL
            ,p_job_monitor_id
            ,l_c_job_name
            ,upper(p_stage_name_in)
            ,'RUNNING'
            ,NULL
            ,SYSDATE
            ,NULL
            ,NULL
            ,upper(p_key_table_name_in)
            ,NULL
        FROM dual;
    COMMIT;
  
  END sp_start_stage_monitoring;

  PROCEDURE sp_end_stage_monitoring(p_job_monitor_id             NUMBER
                                   ,p_key_table_name_in          VARCHAR2
                                   ,p_report_date_column_name_in VARCHAR2 DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_i_job_stage_id       NUMBER(10);
    l_i_key_table_rowcount PLS_INTEGER;
  BEGIN
  
    l_i_job_stage_id       := get_last_job_stage_id(p_job_monitor_id);
    l_i_key_table_rowcount := get_table_rowcount(p_key_table_name_in, p_report_date_column_name_in);
  
    UPDATE etl_job_stage_monitor
       SET stage_status       = 'SUCCESSFULLY ENDED'
          ,end_time           = SYSDATE
          ,operating_time     = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                                to_dsinterval(to_char(start_time, 'DD HH24:MI:SS'))
          ,key_table_name     = upper(p_key_table_name_in)
          ,key_table_rowcount = l_i_key_table_rowcount
     WHERE etl_job_stage_monitor.job_stage_id = l_i_job_stage_id
           AND etl_job_stage_monitor.job_monitor_id = p_job_monitor_id;
    COMMIT;
  
  END sp_end_stage_monitoring;
  
  FUNCTION sf_is_stage_successfully_ended(p_job_id_in         NUMBER
                                         ,p_stage_name_in     VARCHAR2) RETURN NUMBER IS
  l_i_answer_out pls_integer;
  BEGIN
    SELECT CASE job_status WHEN 'SUCCESSFULLY ENDED' THEN  1
                           WHEN 'ENDED WITH ERRORS'  THEN -1 
                           WHEN 'ERROR'              THEN -1
                           ELSE                            0 
            END answer
      INTO l_i_answer_out
      FROM etl_job_monitor
      JOIN etl_job_stage_monitor on etl_job_stage_monitor.job_monitor_id = etl_job_monitor.job_monitor_id
     WHERE etl_job_monitor.start_time_job = (SELECT MAX(start_time_job) FROM etl_job_monitor WHERE job_id = p_job_id_in)
       AND etl_job_stage_monitor.stage_name = p_stage_name_in;
     
    RETURN l_i_answer_out;
  EXCEPTION 
  WHEN OTHERS THEN RETURN -10;
  END sf_is_stage_successfully_ended;
  
  PROCEDURE sp_report_stage_error(p_job_monitor_id     NUMBER
                                 ,p_c_error_message_in VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_i_job_stage_id NUMBER(10);
  BEGIN
    g_i_is_error_occured := 1;
    l_i_job_stage_id   := get_last_job_stage_id(p_job_monitor_id);
  
    UPDATE etl_job_monitor
       SET job_status         = 'ERROR'
          ,end_time_job       = SYSDATE
          ,operating_time_job = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                                to_dsinterval(to_char(start_time_job, 'DD HH24:MI:SS'))
     WHERE etl_job_monitor.job_monitor_id = p_job_monitor_id;
  
    UPDATE etl_job_stage_monitor
       SET stage_status   = 'ERROR'
          ,end_time       = SYSDATE
          ,operating_time = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                            to_dsinterval(to_char(start_time, 'DD HH24:MI:SS'))
          ,error_report   = p_c_error_message_in
     WHERE etl_job_stage_monitor.job_stage_id = l_i_job_stage_id
           AND etl_job_stage_monitor.job_monitor_id = p_job_monitor_id;
  
    set_job_counters(get_job_id(p_job_monitor_id), 0);
    COMMIT;
  END sp_report_stage_error;

  PROCEDURE sp_report_job_error(p_job_monitor_id     NUMBER
                               ,p_c_error_message_in VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    g_i_is_error_occured := 1;
  
    UPDATE etl_job_monitor
       SET job_status         = 'ERROR'
          ,end_time_job       = SYSDATE
          ,operating_time_job = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                                to_dsinterval(to_char(start_time_job, 'DD HH24:MI:SS'))
          ,error_report       = p_c_error_message_in
     WHERE etl_job_monitor.job_monitor_id = p_job_monitor_id;
  
    set_job_counters(get_job_id(p_job_monitor_id), 0);
    COMMIT;
  END sp_report_job_error;

  PROCEDURE sp_end_job_monitoring(p_job_monitor_id NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_i_job_stage_id NUMBER(10);
    l_c_job_status   VARCHAR2(30);
  BEGIN
  
    l_i_job_stage_id := get_last_job_stage_id(p_job_monitor_id);
    IF g_i_is_error_occured = 1 THEN
      l_c_job_status := 'ENDED WITH ERRORS';
    ELSE
      l_c_job_status := 'SUCCESSFULLY ENDED';
    END IF;
  
    MERGE INTO etl_job_monitor monit
    USING (SELECT et.job_monitor_id
                 ,et.job_stage_id
                 ,SYSDATE end_time_job
                 ,l_c_job_status job_status
                 ,et.key_table_name
                 ,et.key_table_rowcount
             FROM etl_job_stage_monitor et
            WHERE p_job_monitor_id = et.job_monitor_id
                  AND l_i_job_stage_id = et.job_stage_id) temp
    ON (temp.job_monitor_id = monit.job_monitor_id)
    WHEN MATCHED THEN
      UPDATE
         SET monit.job_status         = temp.job_status
            ,monit.operating_time_job = to_dsinterval(to_char(SYSDATE, 'DD HH24:MI:SS')) -
                                        to_dsinterval(to_char(monit.start_time_job, 'DD HH24:MI:SS'))
            ,monit.end_time_job       = temp.end_time_job;
  
    set_job_counters(get_job_id(p_job_monitor_id), 1);
    g_i_is_error_occured := 0;
  
    COMMIT;
  END sp_end_job_monitoring;
  
  FUNCTION sf_is_job_successfully_ended(p_job_id_in NUMBER) RETURN NUMBER AS
  l_i_answer_out pls_integer;
  BEGIN
    SELECT CASE job_status WHEN 'SUCCESSFULLY ENDED' THEN  1
                           WHEN 'ENDED WITH ERRORS'  THEN -1 
                           WHEN 'ERROR'              THEN -1
                           ELSE                            0 
            END answer
      INTO l_i_answer_out
      FROM etl_job_monitor
     WHERE start_time_job = (SELECT MAX(start_time_job) FROM etl_job_monitor WHERE job_id = p_job_id_in);
     
    RETURN l_i_answer_out;
  EXCEPTION 
  WHEN OTHERS THEN RETURN -10;
  END sf_is_job_successfully_ended;
  
  FUNCTION get_job_monitor_id(p_job_id_in NUMBER) RETURN NUMBER AS
  l_n_job_monitor_id_out NUMBER;
  BEGIN
  
    SELECT job_monitor_id
      INTO l_n_job_monitor_id_out
      FROM etl_job_monitor
     WHERE etl_job_monitor.job_id = p_job_id_in
       AND etl_job_monitor.start_time_job = (SELECT MAX(start_time_job) 
                                               FROM etl_job_monitor
                                              WHERE etl_job_monitor.job_id = p_job_id_in);
                                              
    RETURN l_n_job_monitor_id_out;                        
  END;  
  
  FUNCTION get_last_job_stage_id(p_job_monitor_id NUMBER) RETURN NUMBER AS
    l_i_job_stage_id NUMBER(10);
  BEGIN
  
    SELECT MAX(job_stage_id)
      INTO l_i_job_stage_id
      FROM etl_job_stage_monitor
     WHERE etl_job_stage_monitor.job_monitor_id = p_job_monitor_id;
  
    RETURN l_i_job_stage_id;
  END;

  FUNCTION get_job_id(p_job_monitor_id NUMBER) RETURN NUMBER AS
    etl_job_id NUMBER(10);
  BEGIN
    SELECT job_id
      INTO etl_job_id
      FROM etl_job_monitor
     WHERE etl_job_monitor.job_monitor_id = p_job_monitor_id;
  
    RETURN etl_job_id;
  END;

  FUNCTION get_job_name(p_job_id_in NUMBER) RETURN VARCHAR2 AS
    l_c_job_name VARCHAR2(30);
  BEGIN
    SELECT etl_job.job_name
      INTO l_c_job_name
      FROM etl_job
     WHERE etl_job.job_id = p_job_id_in;
  
    RETURN l_c_job_name;
  
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
    
  END get_job_name;

  FUNCTION get_table_rowcount(p_table_name_in              VARCHAR2
                             ,p_report_date_column_name_in VARCHAR2 DEFAULT NULL)
    RETURN NUMBER AS
    l_i_rowcount PLS_INTEGER;
  BEGIN
    IF p_report_date_column_name_in IS NULL THEN
      EXECUTE IMMEDIATE 'select count(1) from ' || p_table_name_in
        INTO l_i_rowcount;
    ELSE
      EXECUTE IMMEDIATE 'select count(1) from ' || p_table_name_in ||
                        'where trunc(sysdate, ''dd'') = trunc(' ||
                        p_report_date_column_name_in || ',''dd'')'
        INTO l_i_rowcount;
    END IF;
  
    RETURN l_i_rowcount;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END get_table_rowcount;

  PROCEDURE set_job_counters(p_job_id_in                NUMBER
                            ,p_is_properly_completed_in NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF p_is_properly_completed_in = 1 THEN
      UPDATE etl_job
         SET etl_job.run_counter = etl_job.run_counter + 1
       WHERE etl_job.job_id = p_job_id_in;
    ELSE
      UPDATE etl_job
         SET etl_job.run_counter   = etl_job.run_counter + 1
            ,etl_job.error_counter = etl_job.error_counter + 1
       WHERE etl_job.job_id = p_job_id_in;
    END IF;
    COMMIT;
  END set_job_counters;

/*  PROCEDURE set_debug_mode_on AS
  BEGIN
    g_debug_mode_status := 1;
  END set_debug_mode_on;

  PROCEDURE set_debug_mode_off AS
  BEGIN
    g_debug_mode_status := 0;
  END set_debug_mode_off;*/

END etl_job_monitor_pkg;
