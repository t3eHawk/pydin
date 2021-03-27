/*******************************************************************************
Script to deploy a devoe DB schema in your Oracle environment.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table de_schedule (
  id          number(*, 0),
  job         varchar2(30 char),
  description varchar2(30 char),
  status      varchar2(1 char) default 'N',
  monthday    varchar2(100 char),
  weekday     varchar2(100 char),
  hour        varchar2(100 char),
  minute      varchar2(100 char),
  second      varchar2(100 char),
  trigger_id  number(*, 0),
  start_date  date,
  end_date    date,
  environment varchar2(50 char),
  arguments   varchar2(100 char),
  timeout     number(*, 0),
  maxreruns   number(*, 0),
  maxdays     number(*, 0),
  recipients  varchar2(100 char),
  alarm       varchar2(1 char),
  debug       varchar2(1 char),
  created     date,
  updated     date,
  constraint de_schedule_pk primary key (id)
);

create sequence de_schedule_seq
increment by 1
start with 1
nocache;

create or replace trigger de_schedule_id_trg
before insert on de_schedule
for each row
begin
  select de_schedule_seq.nextval into :new.id from dual;
end;
/

create table de_run_history (
  id          number(*, 0),
  job_id      number(*, 0),
  run_mode    varchar2(25 char),
  run_tag     number(*, 0),
  run_date    date,
  added       date,
  start_date  date,
  end_date    date,
  status      varchar2(1 char),
  server      varchar2(25 char),
  user        varchar2(25 char),
  pid         number(*, 0),
  trigger_id  number(*, 0),
  rerun_id    number(*, 0),
  rerun_seqno number(*, 0),
  rerun_times number(*, 0),
  rerun_now   varchar2(1 char),
  rerun_done  varchar2(1 char),
  file_log    varchar2(25 char),
  text_log    clob,
  text_error  clob,
  data_dump   blob,
  updated     date,
  constraint de_run_history_pk primary key (id)
);

create index de_run_history_job_ix
on de_run_history (job_id);

create index de_run_history_start_date_ix
on de_run_history (start_date);

create index de_run_history_status_ix
on de_run_history (status);

create sequence de_run_history_seq
increment by 1
start with 1
nocache;

create or replace trigger de_run_history_id_trg
before insert on de_run_history
for each row
begin
  select de_run_history_seq.nextval into :new.id from dual;
end;
/

create table de_task_history (
  id              number(*, 0),
  job_id          number(*, 0),
  run_id          number(*, 0),
  task_name       varchar2(30 char),
  task_date       date,
  start_date      date,
  end_date        date,
  status          varchar2(1 char),
  records_read    number(*, 0),
  records_written number(*, 0),
  records_error   number(*, 0),
  result_value    number(*, 0),
  result_long     varchar2(4000 char),
  updated         date,
  constraint de_task_history_pk primary key (id)
);

create sequence de_task_history_seq
increment by 1
start with 1
nocache;

create or replace trigger de_task_history_id_trg
before insert on de_task_history
for each row
begin
  select de_task_history_seq.nextval into :new.id from dual;
end;
/

create table de_step_history (
  id              number(*, 0),
  job_id          number(*, 0),
  run_id          number(*, 0),
  task_id         number(*, 0),
  step_name       varchar2(30 char),
  step_type       varchar2(3 char),
  step_a          varchar2(30 char),
  step_b          varchar2(30 char),
  step_c          varchar2(30 char),
  step_date       date,
  start_date      date,
  end_date        date,
  status          varchar2(1 char),
  records_read    number(*, 0),
  records_written number(*, 0),
  records_error   number(*, 0),
  result_value    number(*, 0),
  result_long     varchar2(4000 char),
  updated         date,
  constraint de_step_history_pk primary key (id)
);

create sequence de_step_history_seq
increment by 1
start with 1
nocache;

create or replace trigger de_step_history_id_trg
before insert on de_step_history
for each row
begin
  select de_step_history_seq.nextval into :new.id from dual;
end;
/

create table de_file_log (
  id         number(*, 0),
  job_id     number(*, 0),
  run_id     number(*, 0),
  task_id    number(*, 0),
  step_id    number(*, 0),
  server     varchar2(25 char),
  file_name  varchar2(50 char),
  file_date  date,
  file_size  number(*, 0),
  start_date date,
  end_date   date,
  constraint de_file_log_pk primary key (id)
);

create sequence de_file_log_seq
increment by 1
start with 1
nocache;

create or replace trigger de_file_log_id_trg
before insert on de_file_log
for each row
begin
  select de_file_log_seq.nextval into :new.id from dual;
end;
/

create table de_sql_log (
  id          number(*, 0),
  job_id      number(*, 0),
  run_id      number(*, 0),
  task_id     number(*, 0),
  step_id     number(*, 0),
  db_name     varchar2(128 char),
  schema_name varchar2(128 char),
  table_name  varchar2(128 char),
  query_type  varchar2(10 char),
  query_text  varchar2(4000 char),
  start_date  date,
  end_date    date,
  output_rows number(*, 0),
  output_text varchar2(4000 char),
  error_code  number(*, 0),
  error_text  varchar2(4000 char),
  constraint de_sql_log_pk primary key (id)
);

create sequence de_sql_log_seq
increment by 1
start with 1
nocache;

create or replace trigger de_sql_log_id_trg
before insert on de_sql_log
for each row
begin
  select de_sql_log_seq.nextval into :new.id from dual;
end;
/

create table de_components (
  id         varchar2(100 char),
  server     varchar2(100 char),
  user       varchar2(100 char),
  pid        number(*, 0),
  url        varchar2(100 char),
  debug      char(1 char),
  start_date date,
  stop_date  date,
  status     varchar2(1 char),
  constraint de_components_pk primary key (id)
);

insert into de_components (id) values ('SCHEDULER');
insert into de_components (id) values ('RESTAPI');
commit;
