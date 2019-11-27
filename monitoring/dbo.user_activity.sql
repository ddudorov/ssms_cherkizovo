----------------------
-- data create: 201911
-- name project/database: user_activity
-- author: RoshchinMU
-- description: Таблица и хранимка для записи действий пользователей
---------------------

use monitoring; go;

go

--drop table monitoring.dbo.user_activity
create table monitoring.dbo.user_activity
(
		 id							int				not	null	IDENTITY(1,1)	
		,login_user					varchar(150)	not null 	default UPPER(ORIGINAL_LOGIN())		
		,start_dt_tm				datetime		not null 	default getdate()
		
		,end_dt_tm					datetime			null 
		,between_start_end_dt_tm	as end_dt_tm - start_dt_tm
		,project_name				varchar(150)	not null
		,project_action				varchar(250)		null
		,project_details			varchar(350)		null

);



go


alter procedure dbo.user_activity_insert  @id				int			 = null
										 ,@project_name		varchar(150) = 'отсутствует наименование'
										 ,@project_action	varchar(250) = null
										 ,@project_details	varchar(350) = null							
as
BEGIN
			SET NOCOUNT ON;
	
			if not @id is null 
			begin
					update monitoring.dbo.user_activity 
					set end_dt_tm = GETDATE()
					where id = @id;
			end
			else
			begin

					insert into monitoring.dbo.user_activity ( project_name,  project_action,  project_details)	
													   values(@project_name, @project_action, @project_details);

					select @@identity as 'identity',scope_identity() as 'scope_identity', ident_current( 'user_activity' )  as 'ident_current'

			end;

end;

go


exec  monitoring.dbo.user_activity_insert





select * from monitoring.dbo.user_activity

