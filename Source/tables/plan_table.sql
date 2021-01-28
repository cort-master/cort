---- TABLE PLAN_TABLE ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'PLAN_TABLE') LOOP
    EXECUTE IMMEDIATE 'drop table '||x.table_name;
  END LOOP;
END;
/  


CREATE GLOBAL TEMPORARY TABLE PLAN_TABLE(
  statement_id       VARCHAR2(30),
  plan_id            NUMBER,
  timestamp          DATE,
  remarks            VARCHAR2(4000),
  operation          VARCHAR2(4000),
  options            VARCHAR2(255),
  object_node        VARCHAR2(128),
  object_owner       VARCHAR2(30),
  object_name        VARCHAR2(30),
  object_alias       VARCHAR2(65),
  object_instance    INTEGER,
  object_type        VARCHAR2(30),
  optimizer          VARCHAR2(255),
  search_columns     NUMBER,
  id                 INTEGER,
  parent_id          INTEGER,
  depth              INTEGER,
  position           INTEGER,
  cost               INTEGER,
  cardinality        INTEGER,
  bytes              INTEGER,
  other_tag          VARCHAR2(255),
  partition_start    VARCHAR2(255),
  partition_stop     VARCHAR2(255),
  partition_id       INTEGER,
  other              LONG,
  other_xml          CLOB,
  distribution       VARCHAR2(30),
  cpu_cost           INTEGER,
  io_cost            INTEGER,
  temp_space         INTEGER,
  access_predicates  VARCHAR2(4000),
  filter_predicates  VARCHAR2(4000),
  projection         VARCHAR2(4000),
  time               INTEGER,
  qblock_name        VARCHAR2(30)
)
ON COMMIT PRESERVE ROWS
;

GRANT SELECT, INSERT, UPDATE, DELETE ON plan_table TO PUBLIC;

