The file "toggl_test.py" contains tested code on fetching data from the jobs USA url and uploading to a database
the code works in three stages:
 >*Fetch data
 >>*Process data and Write to S3 
 >>>*Upload s3 file to a table in Redshift
>>>>NOTE: my mac os doesnt support docker so I proceeded with a cloud solution. 

**URL documentation:**
I used the following URL to fetch data:
"https://data.usajobs.gov/api/search?PositionTitle=Data&LocationName=Omaha,%20Nebraska"
I used Omaha as Chicago didnt show up any results and Data as a keyword as "Data engineer" didnt return anything as well(likely too specific)

**Authentication:**
All the secrets needed for authenticaion of various services are in the secrets.txt file

**Redshift documentation:**
I spun up a cluster with 1 node and created a table called "jobs_psotings" with the following statement:
>create table jobs_postings(
  title varchar(255),
  uri varchar(255),
  location varchar(255),
  salary_min float,
  salary_max float
)
>>This stores the data from the processed S3 file uploaded via the COPY command. A screenshot of the data loaded in the table is in the repo as well
