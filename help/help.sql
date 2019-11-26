
select FORMAT(123456666789,'0-00-0') 123 123
select convert(dec(4,3),0.02)
select ISNUMERIC('134')
select ORIGINAL_LOGIN()					

select FLOOR(1.256)						-- îêðóãëåíèå âíèç / îòáðàñûâàåò äðîáíóþ ÷àñòü
select CEILING(1.256)					-- îêðóãëåíèå ââåðõ
select UPPER('ÁîËüØàß ÁóÊâÀ')			-- âåðõíèé ðåãèñòð
select LOWER('ÌàËåÍüÊàß ÁóÊâÀ')			-- íèæíèé ðåãèñòð



select @@version						-- âåðñèÿ ñåðâåðà Microsoft SQL Server 2017 (RTM-CU13-OD) (KB4483666) - 14.0.3049.1 (X64)  on Windows Server 2016 Standard 10.0 <X64> (Build 14393: ) (Hypervisor) 
use cherkizovo;							-- ïîäêëþ÷åíèå ê ÁÄ
kill 800								-- çàâåðøèòü ¹ ñåññèþ
select ROUND(15.123456789, 0, 1)        -- îêðóãëåíèå



--------------------
-- ÐÀÁÎÒÀ Ñ ÒÀÁËÈÖÅÉ
--------------------
begin 

			
			-- ïðîâåðêà è óäàëåíèå òàáëèöû
			IF OBJECT_ID('tempdb..#table','U')	is not null drop table #table;
			IF OBJECT_ID('db.dbo.table','U')	is not null drop table db.dbo.[table];

			-- ñîçäàíèå òàáëèöû
			create table #test	
			(	
				 id								int					not	null	IDENTITY(1,1)		
				,id_text						varchar(100)		not	null
				,dt_tm							datetime			not null 	default getdate()
				,year_from_dt_tm												as datepart(year,dt_tm) 
				 PRIMARY KEY (id)
				,UNIQUE (id_text) 
			);
			
			TRUNCATE TABLE #table											-- î÷èñòèòü òàáëèöó

			ALTER TABLE #test ADD			column_name smallint not null;	-- äîáàâëåíèå ñòîáöà
			ALTER TABLE #test DROP COLUMN	column_name;					-- óäàëåíèå ñòîáöà

			ALTER INDEX ALL ON #table DISABLE;								-- îòêëþ÷èòü âñå èíäåêñû
			ALTER INDEX ALL ON #table REBUILD;								-- âêëþ÷èòü  âñå èíäåêñû

			DISABLE TRIGGER Products_INSERT_UPDATE ON Products				
			ENABLE TRIGGER Products_INSERT_UPDATE ON Products				



end;




-----------------------
-- ÐÀÁÎÒÀ Ñ ÏÅÐÅÌÅÍÍÍÎÉ
-----------------------
begin 

			declare @v varchar(10)					-- îáúÿâëåíèå ïåðåìåííûõ
				set @v='Ëåøà'						-- ïðèñâîåíèå çíà÷åíèé
			print @v								-- ðåçóëüòàò
			select @v='Ëåøà1'						-- ïðèñâîåíèå çíà÷åíèé â çàïðîñå
			select @v								-- ðåçóëüòàò

end;



-------------------------
-- ÎÁÐÀÁÎÒÊÀ ÎØÈÁÎÊ Â SQL
-------------------------
begin 

			-- îáðàáîòêà îøèáîê â SQL
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
