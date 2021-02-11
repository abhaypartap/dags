    table_name_dim='dim_user_profiles'
    prefix_dim='dim/'+table_name_dim+'/ptd={{ ds_nodash }}/'
    query1_dim="DROP TABLE IF EXISTS dim_user_profiles"
    query2_dim='''CREATE TABLE IF NOT EXISTS dim_user_profiles
WITH (external_location ='s3://lfdwhv3/dim/dim_user_profiles/',format = 'PARQUET', partitioned_by = ARRAY['ptd'],parquet_compression = 'SNAPPY') AS
select a.user_id,a.pan_name,a.mobile_phone_number,a.pan_number,a.email_address,a.status_code,a.create_date,a.Age,a.gender,a.income,a.user_profile_current_pincode,d.statename as user_profile_current_state,d.districtname as user_profile_current_district,a.permanent_pincode,e.statename as permanent_state,e.districtname as permanent_district,i.pincode as mapmyindia_pincode,i.statename as mapmyindia_state,i.districtname as mapmyindia_district,a.app_version,a.is_fraud,b.name as education,c.name profession,CASE
            WHEN Age >= 21
                 AND Age < 23 THEN '[21,23)'
            WHEN Age >= 23
                 AND Age < 25 THEN '[23,25)'
            WHEN Age >= 25
                 AND Age < 28 THEN '[25,28)'
            WHEN Age >= 28
                 AND Age < 30 THEN '[28,30)'
            WHEN Age >= 30
                 AND Age < 33 THEN '[30,33)'
            WHEN Age >= 33
                 AND Age < 36 THEN '[33,36)'
            WHEN Age >= 36
                 AND Age < 39 THEN '[36,39)'
            WHEN Age >= 39
                 AND Age < 42 THEN '[39,42)'
            WHEN Age >= 42
                 AND Age < 46 THEN '[42,46)'
            WHEN Age >= 46 THEN '45+'
            ELSE ''
        END AS age_bkt,CASE
            WHEN income_source_id = 1 THEN 'Salaried'
            WHEN income_source_id = 2 THEN 'Self-Employed'
            WHEN income_source_id = 3 THEN 'Un-Employed'
            WHEN income_source_id = 4 THEN 'Student'
            ELSE 'NiL'
        END AS Emp_Type,
        case when round(income/12) >= 10000 and round(income/12) <= 15000 then '10000 - 15000'
 when round(income/12) > 15000 and round(income/12) <= 20000 then '15000 - 20000'
 when round(income/12) > 20000 and round(income/12) <= 25000 then '20000 - 25000'
 when round(income/12) > 25000 and round(income/12) <= 30000 then '25000 - 30000'
 when round(income/12) > 30000 and round(income/12) <= 35000 then '30000 - 35000'
 when round(income/12) > 35000 and round(income/12) <= 40000 then '35000 - 40000'
 when round(income/12) > 40000  then '>40000' end as Salary_Range,
 bureau,bureau_score,CASE
 WHEN cast(bureau_score as bigint) >= 300
                 AND cast(bureau_score as bigint) < 600 THEN '[300,600)'
                 WHEN cast(bureau_score as bigint) >= 600
                 AND cast(bureau_score as bigint) < 630 THEN '[600,630)'
            WHEN cast(bureau_score as bigint) >= 630
                 AND cast(bureau_score as bigint) < 680 THEN '[630,680)'
            WHEN cast(bureau_score as bigint) >= 680
                 AND cast(bureau_score as bigint) < 700 THEN '[680,700)'
            WHEN cast(bureau_score as bigint) >= 700
                 AND cast(bureau_score as bigint) < 730 THEN '[700,730)'
            WHEN cast(bureau_score as bigint) >= 730
                 AND cast(bureau_score as bigint) < 760 THEN '[730,760)'
            WHEN cast(bureau_score as bigint) >= 760
                 AND cast(bureau_score as bigint) < 800 THEN '[760,800)'
            WHEN cast(bureau_score as bigint) >= 800 THEN '>=800'
            ELSE 'NiR'
        END AS bureau_score_range,
      credit_vintage_days,
      credit_vintage_months,
      credit_bucket,
        h.is_employed_currently AS pf_employed_currently,
h.pf_t1_contribution AS pf_latest_contribution,SUBSTR(CAST(current_timestamp AT TIME ZONE 'ASIA/KOLKATA' as varchar),1,19) as etl_time,
'{{ ds_nodash }}' as ptd
from
dws.rt_user_profiles a
join (select user_id from dws.rt_loans group by user_id) l on a.user_id=l.user_id
left join dws.education_types b on a.edu_type_id=b.edu_type_id
left join dws.profession_types c on a.prof_type_id=c.prof_type_id
left join dws.all_india_pincode_data d on a.user_profile_current_pincode=d.pincode
left join dws.all_india_pincode_data e on a.permanent_pincode=e.pincode
left join dws.user_mapmyindia_location_data  f on a.user_id=f.user_id
left join dws.all_india_pincode_data i on f.mapmyindia_pincode=i.pincode
left join dws.user_credit_report g on a.user_id=g.user_id and rank=1
left join dws.karza_employment_info h on a.user_id=h.user_id'''
    task_insert_partition1_dim = AWSAthenaOperator(query=query1_dim,database="dim",output_location="s3://lf-athena-query-results/",aws_conn_id="datalake",workgroup = "primary",sleep_time= 30,task_id='drop_table_dim',dag=dag)
    task_insert_partition2_dim = AWSAthenaOperator(query=query2_dim,database="dim",output_location="s3://lf-athena-query-results/",aws_conn_id="datalake",workgroup = "primary",sleep_time= 30,task_id='create_table_dim',dag=dag)
    delete_s3bucket_files_dim = S3DeleteObjectsOperator(task_id='delete_s3bucket_old_files',bucket='lfdwhv3',prefix=prefix_dim,aws_conn_id='datalake',dag=dag)

    delete_s3bucket_files_dim >> task_insert_partition1_dim >> task_insert_partition2_dim

    table_name_dim_all='dim_user_profiles_all'
    prefix_dim_all='dim/'+table_name_dim_all+'/ptd={{ ds_nodash }}/'
    query1_dim_all"DROP TABLE IF EXISTS dim_user_profiles_all"
    query2_dim_all='''CREATE TABLE IF NOT EXISTS dim_user_profiles_all
WITH (external_location ='s3://lfdwhv3/dim/dim_user_profiles_all/',format = 'PARQUET', partitioned_by = ARRAY['ptd'],parquet_compression = 'SNAPPY') AS
select a.user_id,a.pan_name,a.mobile_phone_number,a.pan_number,a.email_address,a.status_code,a.create_date,a.Age,a.gender,a.income,a.user_profile_current_pincode,d.statename as user_profile_current_state,d.districtname as user_profile_current_district,a.permanent_pincode,e.statename as permanent_state,e.districtname as permanent_district,i.pincode as mapmyindia_pincode,i.statename as mapmyindia_state,i.districtname as mapmyindia_district,a.app_version,a.is_fraud,b.name as education,c.name profession,CASE
            WHEN Age >= 21
                 AND Age < 23 THEN '[21,23)'
            WHEN Age >= 23
                 AND Age < 25 THEN '[23,25)'
            WHEN Age >= 25
                 AND Age < 28 THEN '[25,28)'
            WHEN Age >= 28
                 AND Age < 30 THEN '[28,30)'
            WHEN Age >= 30
                 AND Age < 33 THEN '[30,33)'
            WHEN Age >= 33
                 AND Age < 36 THEN '[33,36)'
            WHEN Age >= 36
                 AND Age < 39 THEN '[36,39)'
            WHEN Age >= 39
                 AND Age < 42 THEN '[39,42)'
            WHEN Age >= 42
                 AND Age < 46 THEN '[42,46)'
            WHEN Age >= 46 THEN '45+'
            ELSE ''
        END AS age_bkt,CASE
            WHEN income_source_id = 1 THEN 'Salaried'
            WHEN income_source_id = 2 THEN 'Self-Employed'
            WHEN income_source_id = 3 THEN 'Un-Employed'
            WHEN income_source_id = 4 THEN 'Student'
            ELSE 'NiL'
        END AS Emp_Type,
        case when round(income/12) >= 10000 and round(income/12) <= 15000 then '10000 - 15000'
 when round(income/12) > 15000 and round(income/12) <= 20000 then '15000 - 20000'
 when round(income/12) > 20000 and round(income/12) <= 25000 then '20000 - 25000'
 when round(income/12) > 25000 and round(income/12) <= 30000 then '25000 - 30000'
 when round(income/12) > 30000 and round(income/12) <= 35000 then '30000 - 35000'
 when round(income/12) > 35000 and round(income/12) <= 40000 then '35000 - 40000'
 when round(income/12) > 40000  then '>40000' end as Salary_Range,
 bureau,bureau_score,CASE 
 WHEN cast(bureau_score as bigint) >= 300
                 AND cast(bureau_score as bigint) < 600 THEN '[300,600)'
                 WHEN cast(bureau_score as bigint) >= 600
                 AND cast(bureau_score as bigint) < 630 THEN '[600,630)'
            WHEN cast(bureau_score as bigint) >= 630
                 AND cast(bureau_score as bigint) < 680 THEN '[630,680)'
            WHEN cast(bureau_score as bigint) >= 680
                 AND cast(bureau_score as bigint) < 700 THEN '[680,700)'
            WHEN cast(bureau_score as bigint) >= 700
                 AND cast(bureau_score as bigint) < 730 THEN '[700,730)'
            WHEN cast(bureau_score as bigint) >= 730
                 AND cast(bureau_score as bigint) < 760 THEN '[730,760)'
            WHEN cast(bureau_score as bigint) >= 760
                 AND cast(bureau_score as bigint) < 800 THEN '[760,800)'
            WHEN cast(bureau_score as bigint) >= 800 THEN '>=800'
            ELSE 'NiR'
        END AS bureau_score_range,
      credit_vintage_days,
      credit_vintage_months,
      credit_bucket,
        h.is_employed_currently AS pf_employed_currently,
h.pf_t1_contribution AS pf_latest_contribution,SUBSTR(CAST(current_timestamp AT TIME ZONE 'ASIA/KOLKATA' as varchar),1,19) as etl_time,
'{{ ds_nodash }}' as ptd
        
from
dws.rt_user_profiles a
left join dws.education_types b on a.edu_type_id=b.edu_type_id
left join dws.profession_types c on a.prof_type_id=c.prof_type_id
left join dws.all_india_pincode_data d on a.user_profile_current_pincode=d.pincode
left join dws.all_india_pincode_data e on a.permanent_pincode=e.pincode
left join dws.user_mapmyindia_location_data  f on a.user_id=f.user_id
left join dws.all_india_pincode_data i on f.mapmyindia_pincode=i.pincode
left join dws.user_credit_report g on a.user_id=g.user_id and rank=1
left join dws.karza_employment_info h on a.user_id=h.user_id'''
    task_insert_partition1_dim_all = AWSAthenaOperator(query=query1_dim_all,database="dim",output_location="s3://lf-athena-query-results/",aws_conn_id="datalake",workgroup = "primary",sleep_time= 30,task_id='drop_table_dim_all',dag=dag)
    task_insert_partition2_dim_all = AWSAthenaOperator(query=query2_dim_all,database="dim",output_location="s3://lf-athena-query-results/",aws_conn_id="datalake",workgroup = "primary",sleep_time= 30,task_id='create_table_dim_all',dag=dag)
    delete_s3bucket_files_dim_all = S3DeleteObjectsOperator(task_id='delete_s3bucket_old_files_dim_all',bucket='lfdwhv3',prefix=prefix_dim,aws_conn_id='datalake',dag=dag)

    delete_s3bucket_files_dim_all >> task_insert_partition1_dim_all >> task_insert_partition2_dim_all