drop table cherkizovo.info.excel_import_files

select * from cherkizovo.info.excel_import_files

create table cherkizovo.info.excel_import_files
(
	 ex_id				INT				NOT NULL IDENTITY(1,1)
	,data_type			VARCHAR(100)	NOT NULL
	,name_system		VARCHAR(7)		NOT NULL
	,path_file			VARCHAR(300)	NOT NULL
	,date_file			DATETIME		NOT NULL
	,download_user		VARCHAR(50)		NOT NULL DEFAULT ORIGINAL_LOGIN()
	,dt_tm_insert		datetime		NOT NULL DEFAULT getdate()
	,delete_data		bit				NOT NULL DEFAULT 0 -- 0 ������ ���������  / 1 - �������
	 CONSTRAINT [PK excel_import_files | ex_id] PRIMARY KEY CLUSTERED (ex_id) 
)






go

alter procedure info.check_excel_import_files @data_type		VARCHAR(100)
											 ,@name_system		VARCHAR(7)
											 ,@path_file		VARCHAR(300)
											 ,@date_file		DATETIME	
as 
begin

		set nocount on

		-- ��� �����
		/*
		declare @data_type		VARCHAR(100);	set @data_type = '�������'
		declare @name_system	VARCHAR(7);		set @name_system = 'Excel'
		declare @path_file		VARCHAR(300);	set @path_file = '\\cherkizovsky\MOW\MOW03\Users\������������� �����\������������\���� ������������ ��� �� ���� ��\������ ������������ �� ��\�������\������� 10.07.2019.xlsb'
		declare @date_file		DATETIME;		set @date_file = '20190710'
		*/
		-- ������� ������ ������
		--update cherkizovo.info.excel_import_files
		--set delete_data = 1
		--where date_file < getdate() - 180;


		-- ������ ������ (1 - �������) ���� ������ ���� �����
		update cherkizovo.info.excel_import_files
		set delete_data = 1
		where delete_data = 0
			and data_type = @data_type
			and name_system = @name_system
			and date_file = @date_file;


		-- ��������� ����������
		insert into cherkizovo.info.excel_import_files
		(data_type, name_system, path_file, date_file)
		values
		(@data_type, @name_system, @path_file, @date_file);


		-- ������� ������ �� ������
		if Upper(@data_type) = '�������'
		begin

			delete
			from cherkizovo.hist.stuffing
			where ex_id in (select ex_id from cherkizovo.info.excel_import_files
							where data_type = '�������' and delete_data = 1)
		end;

		if Upper(@data_type) = '������'
		begin

			delete
			from cherkizovo.hist.customer_orders
			where ex_id in (select ex_id from cherkizovo.info.excel_import_files
							where data_type = '������' and delete_data = 1)
		end;


		if Upper(@data_type) = '���� ������'
		begin

			delete
			from cherkizovo.hist.sales_plan
			where ex_id in (select ex_id from cherkizovo.info.excel_import_files
							where data_type = '���� ������' and delete_data = 1)
		end;
		

		select @@identity as ex_id;


end


