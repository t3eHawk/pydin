/*******************************************************************************
Script to deploy a PyDin DB schema in your Oracle environment.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table pd_schedule (
  id              number(*, 0),
  job_name        varchar2(50 char),
  job_description varchar2(200 char),
  status          varchar2(1 char) default 'N',
  monthday        varchar2(100 char),
  hour            varchar2(100 char),
  minute          varchar2(100 char),
  second          varchar2(100 char),
  weekday         varchar2(100 char),
  yearday         varchar2(100 char),
  trigger_id      number(*, 0),
  trigger_list    varchar2(100 char),
  start_date      date,
  end_date        date,
  environment     varchar2(50 char),
  arguments       varchar2(100 char),
  timeout         number(*, 0),
  parallelism     varchar2(2 char) default 'N',
  rerun_interval  number(*, 0),
  rerun_limit     number(*, 0),
  rerun_days      number(*, 0),
  rerun_period    varchar2(100 char),
  sleep_period    varchar2(100 char),
  wake_up_period  varchar2(100 char),
  email_list      varchar2(100 char),
  alarm           varchar2(1 char),
  debug           varchar2(1 char),
  created         date,
  updated         date,
  constraint pd_schedule_pk primary key (id)
);

create sequence pd_schedule_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_schedule_id_trg
before insert on pd_schedule
for each row
begin
  select pd_schedule_seq.nextval into :new.id from dual;
end;
/

create or replace trigger pd_schedule_ins_trg
before insert on pd_schedule
for each row
begin
  select sysdate into :new.created from dual;
end;
/

create or replace trigger pd_schedule_upd_trg
before update on pd_schedule
for each row
begin
  select sysdate into :new.updated from dual;
end;
/

create table pd_pipeline_config (
  job_id        number(*, 0) not null,
  pipeline_id   number(*, 0),
  pipeline_name varchar2(50 char),
  pipeline_desc varchar2(200 char),
  error_limit   number(*, 0) default 1,
  query_logging varchar2(1 char) default 'Y',
  file_logging  varchar2(1 char) default 'Y',
  status        varchar2(1 char) default 'Y'
);

create sequence pd_pipeline_config_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_pipeline_config_id_trg
before insert on pd_pipeline_config
for each row
begin
  select pd_pipeline_config_seq.nextval into :new.pipeline_id from dual;
end;
/

create table pd_node_config (
  job_id       number(*, 0) not null,
  pipeline_id  number(*, 0) not null,
  node_id      number(*, 0),
  node_name    varchar2(50 char),
  node_desc    varchar2(200 char),
  node_type    varchar2(50 char) not null,
  node_config  clob,
  source_name  varchar2(50 char),
  custom_query clob,
  date_field   varchar2(50 char),
  days_back    number(*, 0),
  hours_back   number(*, 0),
  months_back  number(*, 0),
  timezone     varchar2(50 char),
  value_field  varchar2(50 char),
  key_field    varchar2(50 char),
  chunk_size   number(*, 0) default 1000,
  cleanup      varchar2(1 char),
  node_seqno   number(*, 0) not null,
  edge_seqlist varchar2(100 char)
);

create sequence pd_node_config_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_node_config_id_trg
before insert on pd_node_config
for each row
begin
  select pd_node_config_seq.nextval into :new.node_id from dual;
end;
/

create table pd_run_history (
  id          number(*, 0),
  job_id      number(*, 0),
  run_mode    varchar2(25 char),
  run_tag     number(*, 0),
  run_date    date,
  added       date,
  start_date  date,
  end_date    date,
  status      varchar2(1 char),
  server_name varchar2(25 char),
  user_name   varchar2(25 char),
  pid         number(*, 0),
  error_list  clob,
  rerun_id    number(*, 0),
  rerun_seqno number(*, 0),
  rerun_times number(*, 0),
  rerun_now   varchar2(1 char),
  rerun_done  varchar2(1 char),
  recycle_ind varchar2(1 char),
  trigger_id  number(*, 0),
  data_dump   blob,
  deactivated date,
  file_log    varchar2(25 char),
  text_log    clob,
  updated     date,
  constraint pd_run_history_pk primary key (id)
);

create index pd_run_history_job_ix
on pd_run_history (job_id);

create index pd_run_history_start_date_ix
on pd_run_history (start_date);

create index pd_run_history_status_ix
on pd_run_history (status);

create sequence pd_run_history_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_run_history_id_trg
before insert on pd_run_history
for each row
begin
  select pd_run_history_seq.nextval into :new.id from dual;
end;
/

create table pd_task_history (
  id                number(*, 0),
  job_id            number(*, 0),
  run_id            number(*, 0),
  task_name         varchar2(50 char),
  task_date         date,
  start_date        date,
  end_date          date,
  status            varchar2(1 char),
  records_read      number(*, 0),
  records_written   number(*, 0),
  records_processed number(*, 0),
  records_error     number(*, 0),
  files_read        number(*, 0),
  files_written     number(*, 0),
  bytes_read        number(*, 0),
  bytes_written     number(*, 0),
  result_value      number(*, 0),
  result_long       varchar2(4000 char),
  updated           date,
  constraint pd_task_history_pk primary key (id)
);

create sequence pd_task_history_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_task_history_id_trg
before insert on pd_task_history
for each row
begin
  select pd_task_history_seq.nextval into :new.id from dual;
end;
/

create table pd_step_history (
  id                number(*, 0),
  job_id            number(*, 0),
  run_id            number(*, 0),
  task_id           number(*, 0),
  step_name         varchar2(50 char),
  step_type         varchar2(3 char),
  step_a            varchar2(50 char),
  step_b            varchar2(50 char),
  step_c            varchar2(50 char),
  step_date         date,
  start_date        date,
  end_date          date,
  status            varchar2(1 char),
  records_read      number(*, 0),
  records_written   number(*, 0),
  records_processed number(*, 0),
  records_error     number(*, 0),
  files_read        number(*, 0),
  files_written     number(*, 0),
  bytes_read        number(*, 0),
  bytes_written     number(*, 0),
  result_value      number(*, 0),
  result_long       varchar2(4000 char),
  updated           date,
  constraint pd_step_history_pk primary key (id)
);

create sequence pd_step_history_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_step_history_id_trg
before insert on pd_step_history
for each row
begin
  select pd_step_history_seq.nextval into :new.id from dual;
end;
/

create table pd_file_log (
  id          number(*, 0),
  job_id      number(*, 0),
  run_id      number(*, 0),
  task_id     number(*, 0),
  step_id     number(*, 0),
  server_name varchar2(50 char),
  file_name   varchar2(200 char),
  file_date   date,
  file_size   number(*, 0),
  start_date  date,
  end_date    date,
  constraint pd_file_log_pk primary key (id)
);

create sequence pd_file_log_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_file_log_id_trg
before insert on pd_file_log
for each row
begin
  select pd_file_log_seq.nextval into :new.id from dual;
end;
/

create table pd_query_log (
  id          number(*, 0),
  job_id      number(*, 0),
  run_id      number(*, 0),
  task_id     number(*, 0),
  step_id     number(*, 0),
  db_name     varchar2(50 char),
  schema_name varchar2(50 char),
  table_name  varchar2(50 char),
  query_type  varchar2(10 char),
  query_text  clob,
  start_date  date,
  end_date    date,
  output_rows number(*, 0),
  output_text varchar2(4000 char),
  error_code  number(*, 0),
  error_text  varchar2(4000 char),
  constraint pd_query_log_pk primary key (id)
);

create sequence pd_query_log_seq
increment by 1
start with 1
nocache;

create or replace trigger pd_query_log_id_trg
before insert on pd_query_log
for each row
begin
  select pd_query_log_seq.nextval into :new.id from dual;
end;
/

create table pd_components (
  id          varchar2(10 char),
  server_name varchar2(50 char),
  user_name   varchar2(50 char),
  pid         number(*, 0),
  url         varchar2(75 char),
  debug       char(1 char),
  start_date  date,
  stop_date   date,
  status      varchar2(1 char),
  constraint pd_components_pk primary key (id)
);

insert into pd_components(id) values ('SCHEDULER');
insert into pd_components(id) values ('RESTAPI');
commit;
