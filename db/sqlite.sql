/*******************************************************************************
Script to deploy a PyDin DB schema in your SQLite environment.
Just run the script then check if all objects created successfully.
*******************************************************************************/

create table pd_schedule (
  id              integer primary key autoincrement,
  job_name        text,
  job_description text,
  status          text default 'N',
  monthday        text,
  hour            text,
  minute          text,
  second          text,
  weekday         text,
  yearday         text,
  trigger_id      integer,
  trigger_list    text,
  start_date      text,
  end_date        text,
  environment     text,
  arguments       text,
  timeout         integer,
  parallelism     text default 'N',
  rerun_interval  integer,
  rerun_limit     integer,
  rerun_days      integer,
  rerun_period    text,
  sleep_period    text,
  wake_up_period  text,
  email_list      text,
  alarm           text,
  debug           text,
  created         text,
  updated         text
);

create trigger pd_schedule_after_insert
  after insert on pd_schedule
begin
  update pd_schedule set created = datetime('now', 'localtime')
   where id = new.id;
end;

create trigger pd_schedule_after_update
  after update on pd_schedule
begin
  update pd_schedule set updated = datetime('now', 'localtime')
   where id = new.id;
end;

create table pd_pipeline_config (
  job_id        integer not null,
  pipeline_id   integer primary key autoincrement,
  pipeline_name text,
  pipeline_desc text,
  error_limit   integer default 1,
  query_logging text default 'Y',
  file_logging  text default 'Y',
  status        text default 'Y'
  unique(job_id)
);

create table pd_node_config (
  job_id       integer not null,
  pipeline_id  integer not null,
  node_id      integer primary key autoincrement,
  node_name    text,
  node_desc    text,
  node_type    text not null,
  node_config  text,
  source_name  text,
  custom_query text,
  date_field   text,
  days_back    integer,
  hours_back   integer,
  months_back  integer,
  timezone     text,
  value_field  text,
  key_field    text,
  chunk_size   integer default 1000,
  cleanup      text,
  node_seqno   integer not null,
  edge_seqlist text,
  unique(job_id, pipeline_id, node_seqno)
);

create table pd_run_history (
  id          integer primary key autoincrement,
  job_id      integer,
  run_mode    text,
  run_tag     integer,
  run_date    text,
  added       text,
  start_date  text,
  end_date    text,
  status      text,
  server_name text,
  user_name   text,
  pid         integer,
  error_list  text,
  rerun_id    integer,
  rerun_seqno integer,
  rerun_times integer,
  rerun_now   text,
  rerun_done  text,
  recycle_ind text,
  deactivated text,
  trigger_id  integer,
  data_dump   blob,
  file_log    text,
  text_log    text,
  updated     text
);

create index pd_run_history_job_ix
on pd_run_history (job_id);

create index pd_run_history_start_date_ix
on pd_run_history (start_date);

create index pd_run_history_status_ix
on pd_run_history (status);

create table pd_task_history (
  id                integer primary key autoincrement,
  job_id            integer,
  run_id            integer,
  task_name         text,
  task_date         text,
  start_date        text,
  end_date          text,
  status            text,
  records_read      integer,
  records_written   integer,
  records_processed integer,
  records_error     integer,
  files_read        integer,
  files_written     integer,
  bytes_read        integer,
  bytes_written     integer,
  result_value      integer,
  result_long       text,
  updated           text
);

create table pd_step_history (
  id                integer primary key autoincrement,
  job_id            integer,
  run_id            integer,
  task_id           integer,
  step_name         text,
  step_type         text,
  step_a            text,
  step_b            text,
  step_c            text,
  step_date         text,
  start_date        text,
  end_date          text,
  status            text,
  records_read      integer,
  records_written   integer,
  records_processed integer,
  records_error     integer,
  files_read        integer,
  files_written     integer,
  bytes_read        integer,
  bytes_written     integer,
  result_value      integer,
  result_long       text,
  updated           text
);

create table pd_file_log (
  id          integer primary key autoincrement,
  job_id      integer,
  run_id      integer,
  task_id     integer,
  step_id     integer,
  server_name text,
  file_name   text,
  file_date   text,
  file_size   integer,
  start_date  text,
  end_date    text
);

create table pd_query_log (
  id          integer primary key autoincrement,
  job_id      integer,
  run_id      integer,
  task_id     integer,
  step_id     integer,
  db_name     text,
  schema_name text,
  table_name  text,
  query_type  text,
  query_text  text,
  start_date  text,
  end_date    text,
  output_rows integer,
  output_text text,
  error_code  integer,
  error_text  text
);

create table pd_components (
  id          text primary key,
  server_name text,
  user_name   text,
  pid         integer,
  url         text,
  debug       text,
  start_date  text,
  stop_date   text,
  status      text
);

insert into pd_components(id) values ('SCHEDULER');
insert into pd_components(id) values ('RESTAPI');
commit;
