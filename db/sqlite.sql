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

create table de_run_history (
  id          integer primary key autoincrement,
  job_id      integer,
  job_date    text,
  added       text,
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
  text_error  text,
  updated     text,
);

create index de_run_history_job_id_ix
on de_run_history (job_id);

create index de_run_history_start_date_ix
on de_run_history (start_date);

create index de_run_history_status_ix
on de_run_history (status);

create table de_task_history (
  id              integer primary key autoincrement,
  job_id          integer,
  run_id          integer,
  task_name       text,
  task_date       text,
  start_date      text,
  end_date        text,
  status          text,
  records_read    integer,
  records_written integer,
  errors_found    integer,
  result_value    integer,
  result_long     integer,
  updated         text
);

create table de_step_history (
  id              integer primary key autoincrement,
  job_id          integer,
  run_id          integer,
  task_id         integer,
  step_name       text,
  step_type       text,
  step_a          text,
  step_b          text,
  step_c          text,
  step_date       text,
  start_date      text,
  end_date        text,
  status          text,
  records_read    integer,
  records_written integer,
  errors_found    integer,
  result_value    integer,
  result_long     text,
  updated         text
);

create table de_file_history (
  id          integer primary key autoincrement,
  job_id      integer,
  run_id      integer,
  task_id     integer,
  step_id     integer,
  server      text,
  file_name   text,
  file_date   text,
  file_size   integer,
  start_date  text,
  end_date    text
);

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
