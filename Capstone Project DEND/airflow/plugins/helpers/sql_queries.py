# Defining SQLQueries class calling the required table creations and respective transformations
class SqlQueries:
    immigration_table_insert = ("""
        SELECT 
			cicid as iid, 
			cicid as nmid, 
			coalesce(ident,'0') as airportcode,
			fltno, 
			name as airportname,
			cast(DATEADD(day,round(coalesce(arrdate,'0'))::int,'19600101') as date) as arrdate,
			cast(DATEADD(day,round(coalesce(depdate,'0'))::int,'19600101') as date) as depdate,
			cast(DATEADD(day,round(coalesce(depdate,'0'))::int,'19600101') as date) - cast(DATEADD(day,round(coalesce(arrdate,'0'))::int,'19600101') as date) as length_of_stay,
			occup as occupation,
			visatype,
			coalesce(trim(i94cit),'0') as native_code
			FROM staging_immigration s left outer join staging_airportcodes t on trim(s.airline)=substring(trim(t.ident),3,2) where trim(depdate) <> ''
    """)

    airports_table_insert = ("""
        SELECT distinct ident as airportcode, name, type, iso_country as country, municipality, gps_code, elevation_ft 
        FROM staging_airportcodes
    """)

    non_immigrants_table_insert = ("""
        SELECT distinct cicid as nmid, coalesce(trim(i94bir),'0') as age, gender, coalesce(trim(biryear) ,'0') as biryear, occup as occupation, visatype, i94addr as address, admnum, trim(i94cit) as native_code 
        FROM staging_immigration
    """)
   
    flights_table_insert = ("""
        SELECT distinct cicid as flightid, airline, fltno , cast(DATEADD(day,round(coalesce(arrdate,'0'))::int,'19600101') as date) as arrdate, cast(DATEADD(day,round(coalesce(depdate,'0'))::int,'19600101') as date) as depdate,i94mode as mode 
        FROM staging_immigration where trim(depdate) <>''
    """)

    demographics_table_insert = ("""
        SELECT distinct cast(t.dt as date), t.city,s.state_code,t.latitude,t.longitude,s.race,s.male_population,s.female_population,s.total_population,s.foreign_born 
        FROM staging_temperature t left outer join staging_usdemographics s on trim(t.city)=trim(s.city) where t.country='United States'
    """)
    
    create_immigration_staging_table = ("""
    DROP TABLE IF EXISTS staging_immigration;CREATE TABLE IF NOT EXISTS staging_immigration (
    index varchar(100),
    cicid  varchar(100) ,
	i94yr  varchar(100) ,
 	i94mon  varchar(100) ,
 	i94cit  varchar(100) ,
 	i94res  varchar(100) ,
 	i94port varchar(256) ,
 	arrdate  varchar(100) ,
 	i94mode  varchar(100) ,
 	i94addr varchar(256) ,
 	depdate  varchar(100) ,
 	i94bir  varchar(100) ,
 	i94visa  varchar(100) ,
 	count  varchar(100) ,
 	dtadfile varchar(256) ,
 	visapost varchar(256) ,
 	occup varchar(256) ,
 	entdepa varchar(256) ,
 	entdepd varchar(256) ,
 	entdepu varchar(256) ,
 	matflag varchar(256) ,
 	biryear  varchar(100) ,
 	dtaddto varchar(256) ,
 	gender varchar(256) ,
 	insnum varchar(256) ,
 	airline varchar(256) ,
 	admnum  varchar(100) ,
 	fltno varchar(256) ,
 	visatype varchar(256)
        );
    """)

    create_temperature_staging_table = ("""
    DROP TABLE IF EXISTS staging_temperature;CREATE TABLE IF NOT EXISTS staging_temperature(
	index varchar(100),
    dt varchar(100),
	AverageTemperature float,
	AverageTemperatureUncertainty float,
	City varchar(100),
	Country varchar(100),
	Latitude varchar(100),
	Longitude varchar(100)
        );
    """)

    create_usdemographics_staging_table = ("""
    DROP TABLE IF EXISTS staging_usdemographics;CREATE TABLE IF NOT EXISTS staging_usdemographics(
    index varchar(100),
	City varchar(100),
	State varchar(100),
	Median_Age float,
    Male_Population float,
	Female_Population float,
    Total_Population float,
    Number_of_Veterans float,
	Foreign_born float,
    Average_Household_Size float,
	State_Code varchar(100),
	Race varchar(100),
	Count int
        );
    """)

    create_airportcodes_staging_table = ("""
    DROP TABLE IF EXISTS staging_airportcodes;CREATE TABLE IF NOT EXISTS staging_airportcodes(
	index varchar(100),
	ident varchar(100),
	type varchar(100),
	name varchar(100),
    elevation_ft float,
	continent varchar(100),
	iso_country varchar(100),
	iso_region  varchar(100),
	municipality varchar(100),
	gps_code  varchar(100),
	iata_code varchar(100),
	local_code varchar(100),
	latitude  varchar(100),
	longitude varchar(100)
        );
    """)
    
    create_airports_table = ("""
    DROP TABLE IF EXISTS airports;CREATE TABLE IF NOT EXISTS airports (
    aiportcode varchar(100) NOT NULL PRIMARY KEY,
    name varchar(100),
    type varchar(100),
    country varchar(100),
	municipality varchar(100),
	gps_code varchar(100),
    elevation_ft float
        );
    """)
    
    create_non_immigrants_table = ("""
    DROP TABLE IF EXISTS non_immigrants;CREATE TABLE IF NOT EXISTS non_immigrants (
	nmid varchar(100) NOT NULL PRIMARY KEY,
	age varchar(5),
	gender varchar(100),
	biryear varchar(20),
	occupation varchar(100),
	visatype varchar(100),
	address varchar(256),
	admnum varchar(100),
	native_code varchar(50)
	    );
    """)
    
    create_flights_table = ("""
    DROP TABLE IF EXISTS flights;CREATE TABLE IF NOT EXISTS flights (
	flightid varchar(100) NOT NULL PRIMARY KEY,
	airline varchar(100),
	fltno varchar(100),
	arrdate date,
	depdate date,
	mode varchar(100)
	    );
    """)
    
    create_demographics_table = ("""
    DROP TABLE IF EXISTS demographics;CREATE TABLE IF NOT EXISTS demographics (
	dt date NOT NULL,
	city varchar(100),
	state_code varchar(100),
	latitude varchar(100),
	longitude varchar(100),
	race varchar(100),
	male_population float,
	female_population float,
	total_population float,
	foreign_born float,
    CONSTRAINT demographics_pkey PRIMARY KEY (dt,city)
	    );
    """)
        
    create_immigration_table = ("""
    DROP TABLE IF EXISTS immigration;CREATE TABLE IF NOT EXISTS immigration (
	iid varchar(100) NOT NULL PRIMARY KEY,
	nmid varchar(64) NOT NULL,
	airportcode varchar(256) NOT NULL,
	fltno varchar(100),
	airportname varchar(100),
	arrdate date,
	depdate date,
	length_of_stay numeric(18,0),
	occupation varchar(100),
	visatype varchar(100),
	native_code varchar(100)	
	    );
    """)

