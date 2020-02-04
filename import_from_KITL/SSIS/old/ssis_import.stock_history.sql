
/************************************************************************************************/
/* Загрузка истории остатков по дате через цикл													*/
/* Дудоров ДА | 202001																			*/																	
/************************************************************************************************/
CREATE TABLE import_from_KITL_test.import.date
							(
							    id		    INT			IDENTITY(1,1) 
							   ,stock_date	VARCHAR(20)
							   ,stock_time	TIME
							   ,server		VARCHAR(20)
							)

INSERT INTO import_from_KITL_test.import.date (stock_date)
SELECT dt_int
FROM [cherkizovo].[info].[calendar]
WHERE dt_tm BETWEEN '20180201' AND '20180331'


/***********************************************************************************************/


ALTER PROCEDURE import.ssis_sp  (@date NVARCHAR(20))

AS

BEGIN

	Declare @execution_id bigint
	EXEC [SSISDB].[catalog].[create_execution] @package_name=N'ssis_import_stock_pmpk.dtsx'
											  ,@execution_id=@execution_id OUTPUT
											  ,@folder_name=N'import_from_KITL'
											  ,@project_name=N'stock'
											  ,@use32bitruntime=False
											  ,@reference_id=Null
											  ,@runinscaleout=False
	Select @execution_id
	DECLARE @var0 NVARCHAR(20) = @date
	EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id
														   ,@object_type=30
														   ,@parameter_name=N'stock_date'
														   ,@parameter_value=@var0
	DECLARE @var1 smallint = 1
	EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id
														   ,@object_type=50
														   ,@parameter_name=N'LOGGING_LEVEL'
														   ,@parameter_value=@var1
	
	EXEC [SSISDB].[catalog].[start_execution] @execution_id

END


/************************************************************************************************/

DECLARE @sql NVARCHAR(MAX)
DECLARE @id  INT = 0

WHILE @id IS NOT NULL
BEGIN
		WAITFOR DELAY '00:00:05'
		SET @id = @id + 1

		SELECT   @id = MAX(id)
				,@sql = MAX('import.ssis_sp ' + '''' + stock_date + '''')
		FROM import_from_KITL_test.import.date
		WHERE @id = id

		SELECT @sql
		EXEC (@sql)

END

-- Отслеживание выполнения пакета
/************************************************************************************************/

SELECT execution_id, status, project_name, package_name, executed_as_name, created_time,start_time,end_time
FROM [SSISDB].[internal].[execution_info]
ORDER BY execution_id DESC