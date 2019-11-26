use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.transits	

go

alter procedure check_import.transits														
as
BEGIN

			SET NOCOUNT ON;

			-- �� ������������ 1� ����������� SAP ID
			-- ����������� SAP ID � ������ SAP
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			select *, count(s.sap_id) over (partition by s.product_1C_full_name) as check_double_sap_id
			into #sap_id
			from (
					
					select distinct
							 s1.product_1C_full_name
							,s2.sap_id 
							,s2.expiration_date_in_days
							,s2.product_status
							,sm2.stuffing_id
					from cherkizovo.info.products_sap													as s1
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm1 on s1.sap_id = sm1.sap_id
					join cherkizovo.info.products_sap													as s2  on isnull(sm1.SAP_id_correct_manual, sm1.SAP_id) = s2.sap_id 
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm2 on s2.sap_id = sm2.sap_id
				 ) as s;

			update c
			set c.sap_id					= s.SAP_id
				,c.stuffing_id				= s.stuffing_id
				,c.product_status			= s.product_status
				,c.stock_expiration_date	= c.stock_production_date + s.expiration_date_in_days
			from project_plan_production_finished_products.data_import.transits as c
			join #sap_id as s on c.product_1C_full_name = s.product_1C_full_name and s.check_double_sap_id = 1;

			-- ����� ������ ---------------------------------------------------------------
			update project_plan_production_finished_products.data_import.transits
			Set reason_ignore_in_calculate = 
				nullif(
						  case when sap_id is null then '�� ������ sap id | ' else '' end
						+ case when stock_current_KOS is null then '��� ������������ | ' else '' end
						+ case when stock_current_KOS < 0.1 then '��� ������ 10% | ' else '' end
						, '');

			-- ��������� ������ ---------------------------------------------------------------
			select 
					 s.reason_ignore_in_calculate	
					,s.product_status
					,s.sap_id_text
					,s.product_1C_full_name
					,s.stuffing_id
					,s.stock_production_date
					,s.stock_on_date
					,s.stock_expiration_date
					,s.stock_kg	
					,s.stock_current_KOS
					,ie.path_file
					,ie.date_file
					,ie.user_insert
					,ie.dt_tm_insert	
			from project_plan_production_finished_products.data_import.transits as s
			join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table;
		
end;


