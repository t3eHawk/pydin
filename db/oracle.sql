/*******************************************************************************
SQL for devoe DB schema.
*******************************************************************************/

create table de_schedule (
  id          number,
  job_name    varchar2(30),
  job_desc    varchar2(30),
  status      varchar2(1) default 'N',
  monthday    varchar2(100),
  weekday     varchar2(100),
  hour        varchar2(100),
  minute      varchar2(100),
  second      varchar2(100),
  trigger_id  number,
  start_date  date,
  end_date    date,
  environment varchar2(50),
  arguments   varchar2(100),
  timeout     number,
  reruns      number,
  days_rerun  number,
  persons     varchar2(100),
  alarm       varchar2(1),
  debug       varchar2(1),
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

create table de_history (
  id          number,
  job_id      number,
  job_date    date,
  added       date,
  updated     date,
  start_date  date,
  end_date    date,
  status      varchar2(1),
  server      varchar2(25),
  user        varchar2(25),
  pid         number,
  initiator   varchar2(25),
  trigger_id  number,
  rerun_id    number,
  rerun_seqno number,
  rerun_times number,
  rerun_now   varchar2(1),
  rerun_done  varchar2(1),
  file_log    varchar2(25),
  text_log    clob,
  text_error  clob,
  constraint de_history_pk primary key (id)
)
row store compress advanced;

create index de_history_job_id_ix
on de_history (job_id)
compress;

create index de_history_start_date_ix
on de_history (start_date)
compress;

create index de_history_status_ix
on de_history (status)
compress;

create sequence de_history_seq
increment by 1
start with 1
nocache;

create or replace trigger de_history_id_trg
before insert on de_history
for each row
begin
  select de_history_seq.nextval into :new.id from dual;
end;
/

create table de_components (
  code       varchar2(3),
  component  varchar2(50),
  server     varchar2(50),
  user       varchar2(50),
  pid        number,
  start_date date,
  stop_date  date,
  status     varchar2(1),
  constraint de_components_pk primary key (code)
);

insert into de_components (code, component) values ('SHD', 'Scheduler');
insert into de_components (code, component) values ('API', 'API');
commit;
