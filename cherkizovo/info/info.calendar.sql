set language russian;
SET DATEFIRST 1 ;

drop table cherkizovo.info.calendar


create table cherkizovo.info.calendar
(
		 dt_tm						datetime not null
		,dt							as convert(date, dt_tm)
		,dt_int						as convert(int, format(dt_tm,'yyyyMMdd'))

		-- year
		,year_number				as year(dt_tm)
		,year_fisrt_day				as DATETIMEFROMPARTS ( year(dt_tm), 1, 1, 0, 0, 0, 0 )  
		,year_last_day				as DATETIMEFROMPARTS ( year(dt_tm), 12, day( EOMONTH( DATEFROMPARTS( year(dt_tm), 12, 1) )  ) , 0, 0, 0, 0 )  

		,year_month_number			as year(dt_tm) * 100 + month(dt_tm)


		-- month
		,month_number				as month(dt_tm)
		,month_short_name			as left(DATENAME (mm, dt_tm) ,3)
		,month_full_name			as DATENAME (mm, dt_tm)
		,month_fisrt_day			as DATETIMEFROMPARTS ( year(dt_tm), month(dt_tm), 1, 0, 0, 0, 0 )  
		,month_last_day				as DATETIMEFROMPARTS ( year(dt_tm), month(dt_tm), day( EOMONTH( DATEFROMPARTS( year(dt_tm), month(dt_tm), 1) )  ) , 0, 0, 0, 0 )  
		,month_count_days			as day( EOMONTH(dt_tm) )  

		-- week
		,year_week_number			as case 
											when month(dt_tm) = 12 and DATEPART(isowk, dt_tm) in (1)	  then (year(dt_tm) + 1) * 100 + DATEPART(isowk, dt_tm) 
											when month(dt_tm) =  1 and DATEPART(isowk, dt_tm) in (52 ,53) then (year(dt_tm) - 1) * 100 + DATEPART(isowk, dt_tm)  
											else year(dt_tm) * 100 + DATEPART(isowk, dt_tm) 
									   end 
		,week_number				as DATEPART(ISO_WEEK, dt_tm)


		,week_fisrt_day				as DATEADD(day, -DATEPART(dw,dt_tm) + 1 , dt_tm)
		,week_last_day				as DATEADD(day, -DATEPART(dw,dt_tm)		, dt_tm) + 7

		,week_day					as datepart(dw, dt_tm)


		--,[имя номер недели]			as 'W' + right('0' + convert(varchar(2),DATEPART(isowk, dt_tm)),2)

		--,[имя дня недели полное]	as DATENAME(weekday, dt_tm)

		CONSTRAINT [PK calendar |  dt_tm]	PRIMARY KEY CLUSTERED ( dt_tm) 
)



go



declare @datetime datetime;
set @datetime = '20180101';


while  @datetime<'20260101'
begin
	
	insert into cherkizovo.info.calendar (dt_tm)
	values (@datetime)

	set @datetime = DATEADD(day,1,@datetime)

end;

go


select *
from cherkizovo.info.calendar
where isnull(week_number,0) in (1)


--select DATENAME (mm, getdate());

--set language russian;


--use cherkizovo

--create view dbo.[календарь]
--with language russian
--as 
--begin
		

--		select * from cherkizovo.information.[календарь]

--end