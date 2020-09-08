-- 
-- *********************** DIMENSIONAL MODEL: JOB APPLICATIONS [Redshift scripts] ************************
-- ******************************************************************

-- ************************************** dim_applicant
CREATE TABLE dim_applicant
(
 applicant_key varchar(50) PRIMARY KEY,
 firstname     varchar(100),
 lastname      varchar(100), 
 created_date  timestamp 
);

-- ************************************** dim_benefit
CREATE TABLE dim_benefit
(
 benefit_key  varchar(50) PRIMARY KEY,
 benefit_name varchar(50),
 created_date timestamp 
);

-- ************************************** dim_city
CREATE TABLE dim_city
(
 city_key     varchar(50) PRIMARY KEY,
 city_name    varchar(100),
 created_date timestamp
);

-- ************************************** dim_company
CREATE TABLE dim_company
(
 company_key    varchar(50) PRIMARY KEY,
 company_name   varchar(100),
 company_sector varchar(100),
 created_date   timestamp
);

-- ************************************** dim_jobpost
CREATE TABLE dim_jobpost
(
 jobpost_key  varchar(50) PRIMARY KEY,
 jobpost_id   varchar(50),
 advert_id       varchar(50),
 advert_applyurl varchar(2000),
 created_date timestamp
);

-- ************************************** dim_jobtitle
CREATE TABLE dim_jobtitle
(
 jobtitle_key  varchar(50) PRIMARY KEY,
 jobtitle_name varchar(50),
 created_date  timestamp
);

-- ************************************** dim_skill
CREATE TABLE dim_skill
(
 skill_id     varchar(50) PRIMARY KEY,
 skill_name   varchar(50),
 created_date timestamp
);

-- ************************************** dim_status
CREATE TABLE dim_status
(
 status_key   varchar(50) PRIMARY KEY,
 status_name  varchar(50),
 created_date timestamp
);

-- ************************************** dim_time
CREATE TABLE dim_time
(
 time_key	  varchar(50) PRIMARY KEY,
 time_hh24    varchar(10),
 hh24         integer,
 minutes      integer,
 created_date timestamp
);

-- ************************************** dim_date
CREATE TABLE dim_date
(
 date_key       varchar(50) PRIMARY KEY,
 date_value     date,
 day            integer,
 week           integer,
 month          integer,
 year           integer,
 day_name       varchar(15), 
 month_name     varchar(15),
 yyyymmdd       varchar(8),
 created_date   timestamp
);

-- ************************************** bridge_applicant_skills
CREATE TABLE bridge_applicant_skills
(
 applicant_key varchar(50) NOT NULL,
 skill_key      varchar(50) NOT NULL,
 jobpost_key varchar(50) NOT NULL
);

-- ************************************** bridge_jobpost_benefits
CREATE TABLE bridge_jobpost_benefits
(
 jobpost_key varchar(50) NOT NULL,
 benefit_key varchar(50) NOT NULL
);

-- ************************************** fact_job_applications
CREATE TABLE fact_job_applications
(
 jobpost_key          varchar(50) NOT NULL,
 applicant_key        varchar(50) NOT NULL,
 jobtitle_key         varchar(50) NOT NULL,
 company_key          varchar(50) NOT NULL,
 publication_date_key varchar(50) NOT NULL,
 publication_time_key varchar(50) NOT NULL,
 application_date_key varchar(50) NOT NULL,
 application_time_key varchar(50) NOT NULL,
 status_key           varchar(50) NOT NULL,
 city_key             varchar(50) NOT NULL,
 applicant_age        integer NOT NULL,
 applicants_quantity  integer NOT NULL,
 job_quantity         integer NOT NULL,
 activedays           integer NOT NULL,
 created_date         timestamp NOT NULL
);


-- ************************************** INSERT DIM_DATE DATA

INSERT INTO dim_date
with digit as (
    select 0 as d union all 
    select 1 union all select 2 union all select 3 union all
    select 4 union all select 5 union all select 6 union all
    select 7 union all select 8 union all select 9        
),
seq as (
    select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) as num
    from digit a
        cross join digit b
        cross join digit c
        cross join digit d        
),
seqnum as (
    select num 
    from seq
    where num <=getdate()::date -to_date(20100101, 'yyyymmdd')
    union all
    select -num 
    from seq
    where num <= to_date(20221231, 'yyyymmdd')- getdate()::date
    order by 1
)
select to_char((getdate()::date - num)::date, 'YYYYMMDD') as date_key,
(getdate()::date - num)::date as "date",
to_char((getdate()::date - num)::date, 'DD')::integer as day,
to_char((getdate()::date - num)::date, 'W')::integer as week,
to_char((getdate()::date - num)::date, 'MM')::integer as month,
to_char((getdate()::date - num)::date, 'YYYY')::integer as year,
to_char((getdate()::date - num)::date, 'DAY') as day_name,
to_char((getdate()::date - num)::date, 'MONTH') as month_name,
to_char((getdate()::date - num)::date, 'YYYYMMDD') as yyyymmdd,
getdate() created_date
from seqnum
order by 1;

INSERT INTO dim_date (date_key, created_date) values('-1', getdate());
COMMIT;

-- ************************************** INSERT  dim_time DATA

INSERT INTO dim_time
with time_seq as (select dateadd(m,row_number() over (order by true),getdate()::date) as dttm
    from dim_date    
limit 1440)
select to_char(dttm, 'HH24MI') as as time_key, 
       to_char(dttm, 'HH24:MI') as time_hh24, 
       to_char(dttm, 'HH24')::integer as hh24,
       to_char(dttm, 'MI')::integer as mi,
       getdate() created
from time_seq
order by 1
;

INSERT INTO dim_time (time_key, created_date) values('-1', getdate());
COMMIT;

-- ************************************** INSERT DIMs Default values

INSERT INTO dim_benefit VALUES(-1, 'TBD', GETDATE());
INSERT INTO dim_applicant VALUES(-1, 'TBD', 'TBD', NULL, GETDATE());
INSERT INTO dim_city VALUES(-1, 'TBD', GETDATE());
INSERT INTO dim_company VALUES(-1, 'TBD', 'TBD', GETDATE());
INSERT INTO dim_jobpost VALUES(-1, '-1', '-1', 'TBD',GETDATE());
INSERT INTO dim_jobtitle VALUES(-1, 'TBD', GETDATE());
INSERT INTO dim_skill VALUES(-1, 'TBD', GETDATE());
INSERT INTO dim_status VALUES(-1, 'TBD', GETDATE());
COMMIT;