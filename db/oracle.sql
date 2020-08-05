/*******************************************************************************
SQL for devoe DB schema.
*******************************************************************************/

create table de_schedule (
  id          number,
  job_name    varchar2(30 char),
  job_desc    varchar2(30 char),
  status      varchar2(1 char) default 'N',
  monthday    varchar2(100 char),
  weekday     varchar2(100 char),
  hour        varchar2(100 char),
  minute      varchar2(100 char),
  second      varchar2(100 char),
  trigger_id  number,
  start_date  date,
  end_date    date,
  environment varchar2(50 char),
  arguments   varchar2(100 char),
  timeout     number,
  reruns      number,
  days_rerun  number,
  persons     varchar2(100 char),
  alarm       varchar2(1 char),
  debug       varchar2(1 char),
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
  id          number,
  job_id      number,
  job_date    date,
  added       date,
  start_date  date,
  end_date    date,
  status      varchar2(1 char),
  server      varchar2(25 char),
  user        varchar2(25 char),
  pid         number,
  initiator   varchar2(25 char),
  trigger_id  number,
  rerun_id    number,
  rerun_seqno number,
  rerun_times number,
  rerun_now   varchar2(1 char),
  rerun_done  varchar2(1 char),
  file_log    varchar2(25 char),
  text_log    clob,
  text_error  clob,
  updated     date,
  constraint de_run_history_pk primary key (id)
)
row store compress advanced;

create index de_run_history_job_id_ix
on de_run_history (job_id)
compress;

create index de_run_history_start_date_ix
on de_run_history (start_date)
compress;

create index de_run_history_status_ix
on de_run_history (status)
compress;

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
  id              number,
  job_id          number,
  run_id          number,
  task_name       varchar2(30 char),
  task_date       date,
  start_date      date,
  end_date        date,
  status          varchar2(1 char),
  records_read    number,
  records_written number,
  errors_found    number,
  result_value    number,
  result_long     varchar2(512 char),
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
  id              number,
  job_id          number,
  run_id          number,
  task_id         number,
  step_name       varchar2(30 char),
  step_type       varchar2(3 char),
  step_a          varchar2(30 char),
  step_b          varchar2(30 char),
  step_c          varchar2(30 char),
  step_date       date,
  start_date      date,
  end_date        date,
  status          varchar2(1 char),
  records_read    number,
  records_written number,
  errors_found    number,
  result_value    number,
  result_long     varchar2(512 char),
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

create table de_file_history (
  id         number,
  job_id     number,
  run_id     number,
  task_id    number,
  step_id    number,
  server     varchar2(25 char),
  file_name  varchar2(50 char),
  file_date  date,
  file_size  number,
  start_date date,
  end_date   date,
  constraint de_file_history_pk primary key (id)
);

create sequence de_file_history_seq
increment by 1
start with 1
nocache;

create or replace trigger de_file_history_id_trg
before insert on de_file_history
for each row
begin
  select de_file_history_seq.nextval into :new.id from dual;
end;
/

create table de_components (
  code       varchar2(3 char),
  component  varchar2(50 char),
  server     varchar2(50 char),
  user       varchar2(50 char),
  pid        number,
  start_date date,
  stop_date  date,
  status     varchar2(1 char),
  constraint de_components_pk primary key (code)
);

insert into de_components (code, component) values ('SHD', 'Scheduler');
insert into de_components (code, component) values ('API', 'API');
commit;
