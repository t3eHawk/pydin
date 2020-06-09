/*******************************************************************************
SQL for devoe DB schema.
*******************************************************************************/

create table de_schedule (
  id          integer primary key autoincrement,
  job_name    text,
  job_desc    text,
  status      text default 'N',
  monthday    text,
  weekday     text,
  hour        text,
  minute      text,
  second      text,
  trigger_id  integer,
  start_date  text,
  end_date    text,
  environment text,
  arguments   text,
  timeout     integer,
  reruns      integer,
  days_rerun  integer,
  persons     text,
  alarm       text,
  debug       text
);

create table de_history (
  id          integer primary key autoincrement,
  job_id      integer,
  job_date    text,
  added       text,
  updated     text,
  start_date  text,
  end_date    text,
  status      text,
  server      text,
  user        text,
  pid         integer,
  initiator   text,
  trigger_id  integer,
  rerun_id    integer,
  rerun_seqno integer,
  rerun_times integer,
  rerun_now   text,
  rerun_done  text,
  file_log    text,
  text_log    text,
  text_error  text
);

create index de_history_job_id_ix
on de_history (job_id);

create index de_history_start_date_ix
on de_history (start_date);

create index de_history_status_ix
on de_history (status);

create table de_components (
  code       text primary key,
  component  text,
  server     text,
  user       text,
  pid        integer,
  start_date text,
  stop_date  text,
  status     text
);

insert into de_components (code, component) values ('SHD', 'Scheduler');
insert into de_components (code, component) values ('API', 'API');
commit;
