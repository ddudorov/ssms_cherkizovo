test

-- изменить на  UTF-8 файл и тогда все скрипты будут сразу  -- пока не понятно
"C:\Users\m_roshchin\AppData\Roaming\Microsoft\SQL Server Management Studio\14.0\Templates\Sql\SQLFile.sql"


select convert(dec(4,3),0.02) 

-- возрощает номер IDENTITY
select 
 @@identity as 'identity'	 -- уже вставленной строки						
,scope_identity() as 'scope_identity' -- уже вставленной строки		
,ident_current( 'user_activity' )  as 'ident_current' -- последний		



select ORIGINAL_LOGIN()	
select @@version						-- версия сервера Microsoft SQL Server 2017 (RTM-CU15-GDR) (KB4505225) - 14.0.3192.2 (X64)   Jun 15 2019 00:45:05   Copyright (C) 2017 Microsoft Corporation  Enterprise Edition: Core-based Licensing (64-bit) on Windows Server 2016 Standard 10.0 <X64> (Build 14393: ) (Hypervisor) 
use cherkizovo;							-- подключение к БД
kill 800								-- завершить № сессию

select STRING_AGG(convert(varchar(max),id),',')
from cherkizovo.info.products_sap


--------------------
-- РАБОТА С ТАБЛИЦЕЙ
--------------------
begin 

			
			-- проверка и удаление таблицы
			IF OBJECT_ID('tempdb..#table','U')	is not null drop table #table;
			IF OBJECT_ID('db.dbo.table','U')	is not null drop table db.dbo.[table];

			-- создание таблицы
			create table #test	
			(	
				 id								int					not	null	IDENTITY(1,1)		
				,id_text						varchar(100)		not	null
				,dt_tm							datetime			not null 	default getdate()
				,year_from_dt_tm												as datepart(year,dt_tm) 
				 PRIMARY KEY (id)
				,CONSTRAINT [AK #test | id_text] UNIQUE(id_text)
			);
			
			TRUNCATE TABLE #table											-- очистить таблицу всю без условий -- работает быстрее чем delete from [table]

			ALTER TABLE #test ADD			column_name smallint not null;	-- добавление стобца
			ALTER TABLE #test DROP COLUMN	column_name;					-- удаление стобца

			ALTER INDEX ALL ON #table DISABLE;								-- отключить все индексы
			ALTER INDEX ALL ON #table REBUILD;								-- включить  все индексы

			DISABLE TRIGGER Products_INSERT_UPDATE ON Products				
			ENABLE TRIGGER Products_INSERT_UPDATE ON Products				



end;




-----------------------
-- РАБОТА С ПЕРЕМЕНННОЙ
-----------------------
begin 

			declare @v varchar(10)					-- объявление переменных
				set @v='значение'					-- присвоение значений
			print @v								-- результат
			select @v								-- результат

			select @v='значение 2'					-- присвоение значений в запросе
			select @v								-- результат

			set @v = (select top 1 'значение 3')	-- присвоение значений в запросе
			select @v								-- результат


			select FORMAT(10000000,'0-00-0')		-- задает формат числа/даты
			select ISNUMERIC('134')					-- проверка тип данных (число)
			select FLOOR(1.256)						-- округление вниз / отбрасывает дробную часть
			select CEILING(1.256)					-- округление вверх
			select ROUND(15.123456789, 0, 1)        -- округление
			select UPPER('ВеРхнИй РегИсТр')			-- верхний регистр
			select LOWER('НиЖниЙ рЕгиСтР')			-- нижний регистр

end;


-------------------------
-- ОБРАБОТКА ОШИБОК В SQL
-------------------------
begin 

			-- обработка ошибок в SQL
			BEGIN TRY  
						SELECT 1/1;  
						SELECT 1/0;  
			END TRY  
			BEGIN CATCH  
						SELECT  
							 ERROR_NUMBER()		AS ErrorNumber  
							,ERROR_SEVERITY()	AS ErrorSeverity  
							,ERROR_STATE()		AS ErrorState  
							,ERROR_PROCEDURE()	AS ErrorProcedure  
							,ERROR_MESSAGE()	AS ErrorMessage;  
			END CATCH;  
		

end;



----------------------------------------
-- ПРИМЕВЕР ВОЗРАТА ЗНАЧЕНИЯ ИЗ ХРАНИМКИ
----------------------------------------
create procedure #ttt @t int, @t_out int OUTPUT
as
BEGIN
		set @t_out = @t + 1
end;

declare @t int 
set @t = 100
exec #ttt @t = @t, @t_out = @t out

select @t



--------------------------------
-- КАК ПОЛУЧИТЬ ВРЕМЯ ВЫПОЛНЕНИЯ 
--------------------------------
declare @start_tm time(0); set @start_tm = getdate() ;
declare @stop_tm time(0); set @stop_tm = getdate() + 0.12569 ;


select format( DATEDIFF(second,	@start_tm, @stop_tm) / 3600 		,'00') + ':' + 
	   format( DATEDIFF(second, @start_tm, @stop_tm) % 3600 / 60 	,'00') + ':' + 
	   format( DATEDIFF(second, @start_tm, @stop_tm) % 60 			,'00');


select format(5,'00')

select convert(time(0),'12:23:43') + 0.1