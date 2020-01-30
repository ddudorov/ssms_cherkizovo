USE monitoring;

/************************************************/
/* admin.run_vbscript - запуск VBS скриптов		*/
/* Рощин МЮ | 201910						    */
/************************************************/

select * from monitoring.admin.run_script
--drop table monitoring.admin.run_script
CREATE TABLE monitoring.admin.run_script
(
		 id						int				NOT NULL IDENTITY(1,1)
		,run_time				time(0)			NOT NULL DEFAULT getdate()
		,mon					bit				NOT NULL DEFAULT 0
		,tue					bit				NOT NULL DEFAULT 0
		,wed					bit				NOT NULL DEFAULT 0
		,thu					bit				NOT NULL DEFAULT 0
		,fri					bit				NOT NULL DEFAULT 0
		,sat					bit				NOT NULL DEFAULT 0
		,sun					bit				NOT NULL DEFAULT 0
		,run_status				bit				NOT NULL DEFAULT 0
		,description_script		VARCHAR(100)	NOT NULL DEFAULT ''
		,type_script			VARCHAR(50)		NOT NULL CHECK (type_script in ('VBA', 'sql')) 
		,path_script			VARCHAR(500)		NULL 
		,par_1_dt				datetime			null
		,par_2_dt				datetime			null
);








exec monitoring.admin.run_script_check
select * from monitoring.admin.vbscript

update monitoring.admin.vbscript
set run_time = CONVERT (time(0), GETDATE()-0.111) 

update monitoring.admin.vbscript
set run_status = 1
path_vbscript
\\cherkizovsky\mow\MOW01\Users\Automatization\development\MSSQL\data_SAP\Регламент готовой продукции\setting\ZMDG_GOODS_REG.vbs



alter PROCEDURE admin.run_script_check
as
BEGIN
			SET NOCOUNT ON;

			declare @id int;
			declare @path_vbscript VARCHAR(500);
			

			-- ставим на запуск
			update monitoring.admin.run_script
			set run_status = 1
			where run_status = 0
				and run_time > CONVERT (time(0), GETDATE()) 
				and case DATEPART(dw, getdate())
					when 1 then mon
					when 2 then tue
					when 3 then wed
					when 4 then thu
					when 5 then fri
					when 6 then sat
					when 7 then sun
					end = 1;


					
			-- запускаем
			select 
					 @id = isnull(   max(v.id)   ,0)
			from (
					select top 1 id
					from monitoring.admin.run_script
					where run_status = 1
						and run_time <= CONVERT(time(0), GETDATE()) 
						and case DATEPART(dw, getdate())
							when 1 then mon
							when 2 then tue
							when 3 then wed
							when 4 then thu
							when 5 then fri
							when 6 then sat
							when 7 then sun
							end = 1
					order by run_time
				 ) as v;

				 
			-- выключаем
			update monitoring.admin.run_script
			set run_status = 0
			where id = @id;

			---- обновляем остатки
			update monitoring.admin.run_script
			set par_1_dt = par_1_dt + 1
			   ,run_status = 1
			where id = @id and path_script like 'import_stock_%'
			  and par_1_dt < '20190501' and not par_1_dt is null;
			  --and par_1_dt < getdate() and not par_1_dt is null;





			select 
					 isnull(max(type_script),'') as type_script
					,max(path_script) as path_script
					,max(par_1_dt) as par_1_dt
					,max(par_2_dt) as par_2_dt
			from monitoring.admin.run_script
			where id = @id;
			
end;



update [monitoring].[admin].[run_script]
set [run_status] = 0