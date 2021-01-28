DECLARE
  l_sql         VARCHAR2(32767);
BEGIN  
  FOR X IN (SELECT *
              FROM user_triggers
             WHERE trigger_name like 'CORT%'
             ORDER BY trigger_name DESC) 
  LOOP
    l_sql := 'DROP TRIGGER '||x.trigger_name;
    EXECUTE IMMEDIATE l_sql;
  END LOOP;   
END;     
/

