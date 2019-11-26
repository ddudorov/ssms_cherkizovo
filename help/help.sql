
select FORMAT(123456666789,'0-00-0')
select convert(dec(4,3),0.02)
select ISNUMERIC('134')
select ORIGINAL_LOGIN()					

select FLOOR(1.256)						-- округление вниз / отбрасывает дробную часть
select CEILING(1.256)					-- округление вверх
select UPPER('ÁîËüØàß ÁóÊâÀ')			-- верхний регистр
select LOWER('ÌàËåÍüÊàß ÁóÊâÀ')			-- нижний регистр



select @@version						-- версия сервера Microsoft SQL Server 2017 (RTM-CU13-OD) (KB4483666) - 14.0.3049.1 (X64)  on Windows Server 2016 Standard 10.0 <X64> (Build 14393: ) (Hypervisor) 
use cherkizovo;							-- подключение к БД
kill 800								-- завершить № сессию
select ROUND(15.123456789, 0, 1)        -- округление



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
				,UNIQUE (id_text) 
			);
			
			TRUNCATE TABLE #table											-- очистить таблицу

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
				set @v='Ëåøà'						-- присвоение значений
			print @v								-- результат
			select @v='Ëåøà1'						-- присвоение значений в запросе
			select @v								-- результат

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
