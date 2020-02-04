
use project_export_quotation;



--drop table project_export_quotation.dbo.export_finam_ru
create table project_export_quotation.dbo.export_finam_ru
(
		 data_on_date	datetime	not null
		,name_quotation	varchar(50)	not null

		,[market]		varchar(5)	not null	default '1'
        ,[em]			varchar(5)	not null	default '3'
        ,[code]			as name_quotation
        ,[apply]		varchar(5)	not null	default '0'

		,[df]			as convert(varchar(20), day(data_on_date))	
		,[mf]			as convert(varchar(20), month(data_on_date) - 1 )	
		,[yf]			as convert(varchar(20), year(data_on_date))	
        ,[from]			as format(data_on_date,'dd.MM.yyyy')			

		,[dt]			as convert(varchar(20), day(data_on_date))	
		,[mt]			as convert(varchar(20), month(data_on_date) - 1 )		
		,[yt]			as convert(varchar(20), year(data_on_date))	
        ,[to]			as format(data_on_date,'dd.MM.yyyy')			

        ,[p]			varchar(5)	not null	default '1'
        ,[f]			as name_quotation + '_' + format(data_on_date,'yyMMdd') + '_' + format(data_on_date,'yyMMdd') 
        ,[e]			varchar(9)	not null	default '.txt'
        ,[cn]			as name_quotation 
        ,[dtf]			varchar(5)	not null	default '1'
        ,[tmf]			varchar(5)	not null	default '1'
        ,[mstime]		varchar(5)	not null	default 'on'
        ,[mstimever]	varchar(5)	not null	default '1'
        ,[sep]			varchar(5)	not null	default '3'
        ,[sep2]			varchar(5)	not null	default '1'
        ,[datf]			varchar(5)	not null	default '6'
        ,[at]			varchar(5)	not null	default '1'
        ,[fsp]			varchar(5)	not null	default '1'
)


insert into project_export_quotation.dbo.export_finam_ru (data_on_date, name_quotation)  values(getdate()-0, 'SBER')
insert into project_export_quotation.dbo.export_finam_ru (data_on_date, name_quotation)  values(getdate()-1, 'SBER')
insert into project_export_quotation.dbo.export_finam_ru (data_on_date, name_quotation)  values(getdate()-2, 'SBER')
insert into project_export_quotation.dbo.export_finam_ru (data_on_date, name_quotation)  values(getdate()-3, 'SBER')


select *
from project_export_quotation.dbo.export_finam_ru


