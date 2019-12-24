use cherkizovo;

-- drop table cherkizovo.info.calendar
create table cherkizovo.info.calendar
(
		 dt_tm						datetime	not null
		,dt							date		not null
		,dt_int						int			not null

		-- year
		,year_number				smallint	not null
		,year_fisrt_day				date		not null
		,year_last_day				date		not null 

		,year_month_number			int			not null
		
		-- month
		,month_number				tinyint		not null 
		,month_short_name			varchar(3)	not null 
		,month_full_name			varchar(10)	not null 
		,month_fisrt_day			date		not null
		,month_last_day				date		not null 
		,month_count_days			tinyint		not null 

		-- week
		,year_week_number			int			not null
		,week_number				tinyint		not null 
		,week_fisrt_day				date		not null
		,week_last_day				date		not null

		,week_day					tinyint		not null 

		-- day
		,day_type					tinyint		not null 
		,day_type_name				varchar(3)	not null


		CONSTRAINT [PK calendar |  dt_tm]	PRIMARY KEY CLUSTERED ( dt_tm) 
)




set language russian;
SET DATEFIRST 1 ;


declare @datetime datetime;
set @datetime = '20170101';


while  @datetime<'20260101'
begin
	
		insert into cherkizovo.info.calendar 
		select 
			 dt_tm				= @datetime			
			,dt					= @datetime	
			,dt_int				= format(@datetime,'yyyyMMdd')		

			-- year
			,year_number		= year(@datetime)	
			,year_fisrt_day		= DATETIMEFROMPARTS (year(@datetime) + 0, 1, 1, 0, 0, 0, 0 ) 	
			,year_last_day		= DATETIMEFROMPARTS (year(@datetime) + 1, 1, 1, 0, 0, 0, 0 ) - 1

			,year_month_number	= year(@datetime) * 100 + month(@datetime)

			-- month
			,month_number		= month(@datetime)
			,month_short_name	= left(DATENAME (mm, @datetime) ,3)
			,month_full_name	= DATENAME (mm, @datetime)
			,month_fisrt_day	= DATETIMEFROMPARTS ( year(@datetime), month(@datetime), 1, 0, 0, 0, 0 ) 
			,month_last_day		= DATETIMEFROMPARTS ( year(@datetime), month(@datetime), day(EOMONTH(@datetime)), 0, 0, 0, 0 ) 
			,month_count_days	= day( EOMONTH(@datetime) )  
	

			-- week
			,year_week_number	= case 
										when month(@datetime) = 12 and DATEPART(ISO_WEEK, @datetime) in (1)			then (year(@datetime) + 1) * 100 + DATEPART(ISO_WEEK, @datetime)
										when month(@datetime) =  1 and DATEPART(ISO_WEEK, @datetime) in (52 ,53)	then (year(@datetime) - 1) * 100 + DATEPART(ISO_WEEK, @datetime)
										else																			  year(@datetime)	   * 100 + DATEPART(ISO_WEEK, @datetime)
								  end 
			,week_number		= DATEPART(ISO_WEEK, @datetime)
			,week_fisrt_day		= DATEADD(day, - DATEPART(dw, @datetime) + 1, @datetime)
			,week_last_day		= DATEADD(day, - DATEPART(dw, @datetime)	, @datetime) + 7
			,week_day			= DATEPART(dw, @datetime)

			-- day
			,day_type			= case 
										when DATEPART(dw, @datetime) in (1, 2, 3, 4, 5) then 0
										when DATEPART(dw, @datetime) in (6, 7)			then 1
								  end		
			,day_type_name		= case 
										--when @datetime in ('20170101' )then 'Прз'										
										when DATEPART(dw, @datetime) in (1, 2, 3, 4, 5) then 'Раб'
										when DATEPART(dw, @datetime) in (6, 7)			then 'Вых'
								  end

		set @datetime = DATEADD(day,1,@datetime)

end;



select  * from cherkizovo.info.calendar
where week_number in (52,53,1)
order by 1
