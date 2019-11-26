use project_plan_production_finished_products


go

alter procedure check_import.shipments_sales_plan @dates_for_unpivot varchar(max) 										
as
BEGIN
			SET NOCOUNT ON;
	
			declare @sql varchar(max);
			declare @dt_for_delete datetime;
			

			-- ����� �����
			update #sales_plan
			set shipment_sales_channel_name = '������������'
			where shipment_sales_channel_name = '�������������';


			-- ��������� ������
			set @sql = ''
			set @sql = @sql + char(10) + 'insert into project_plan_production_finished_products.data_import.shipments_sales_plan'
			set @sql = @sql + char(10) + '('

			set @sql = @sql + char(10) + '		 shipment_promo_status'
			set @sql = @sql + char(10) + '		,shipment_promo'
			set @sql = @sql + char(10) + '		,shipment_promo_kos_listing'

			set @sql = @sql + char(10) + '		,sap_id_from_excel'
			set @sql = @sql + char(10) + '		,position_dependent_id'
			set @sql = @sql + char(10) + '		,individual_marking_id'
			set @sql = @sql + char(10) + '		,article_nomenclature'
			set @sql = @sql + char(10) + '		,article_packaging'

			set @sql = @sql + char(10) + '		,shipment_sales_channel_name'
			set @sql = @sql + char(10) + '		,shipment_branch_id'
			set @sql = @sql + char(10) + '		,shipment_branch_name'
			set @sql = @sql + char(10) + '		,shipment_customer_id'
			set @sql = @sql + char(10) + '		,shipment_customer_name'
			
			set @sql = @sql + char(10) + '		,shipment_with_branch_date'
			set @sql = @sql + char(10) + '		,shipment_kg'

			set @sql = @sql + char(10) + ')'

			set @sql = @sql + char(10) + 'select' 	
				
			set @sql = @sql + char(10) + '		 upv.shipment_promo_status'
			set @sql = @sql + char(10) + '		,upv.shipment_promo'
			set @sql = @sql + char(10) + '		,upv.shipment_promo_kos_listing'

			set @sql = @sql + char(10) + '		,upv.sap_id_from_excel'
			set @sql = @sql + char(10) + '		,upv.position_dependent_id'
			set @sql = @sql + char(10) + '		,upv.individual_marking_id'
			set @sql = @sql + char(10) + '		,upv.article_nomenclature'
			set @sql = @sql + char(10) + '		,upv.article_packaging'

			set @sql = @sql + char(10) + '		,upv.shipment_sales_channel_name'
			set @sql = @sql + char(10) + '		,upv.shipment_branch_id'
			set @sql = @sql + char(10) + '		,upv.shipment_branch_name'
			set @sql = @sql + char(10) + '		,upv.shipment_customer_id'
			set @sql = @sql + char(10) + '		,upv.shipment_customer_name'
																																	
			set @sql = @sql + char(10) + '		,convert(datetime, RIGHT(upv.shipment_with_branch_date,8)) as shipment_with_branch_date'					
			set @sql = @sql + char(10) + '		,upv.shipment_kg'																							
			set @sql = @sql + char(10) + 'from #sales_plan as upv'
			set @sql = @sql + char(10) + 'UNPIVOT (shipment_kg for shipment_with_branch_date in (' + @dates_for_unpivot + ')) as upv'
			set @sql = @sql + char(10) + 'where upv.shipment_kg <> 0.0'

			exec( @sql);


			-- ������������ �������� �����������
			update project_plan_production_finished_products.data_import.shipments_sales_plan
			set  shipment_promo_status			= trim(shipment_promo_status)
				,shipment_promo_kos_listing		= trim(shipment_promo_kos_listing)
				,shipment_promo					= trim(shipment_promo)
				,shipment_sales_channel_name	= trim(shipment_sales_channel_name)
				,shipment_customer_name			= case trim(shipment_customer_name)
													when '����'					then '�''��� ���'
													when '�''���'				then '�''��� ���'
											
													when 'METRO Group'			then '����� ��� ��� ����� ���'
													when '�����'				then '����� ��� ��� ����� ���'
											
													when 'Lenta'				then '����� ���'
													when '�����'				then '����� ���'
											
													when '�����'				then '����� ���'											
													when 'Billa'				then '����� ���'
													
													when '��� ������ ��'		then '�� ��� ������ ���'
													when '��� ������ �� ���'	then '�� ��� ������ ���'

													when '������'				then '������ ��'
													when '�����������'			then '����������� ���'
													when '�������� ��� ����'	then '�������� ��� ���� ���'
													when '�� ���������'			then '�� ��������� ���'
													when '������ ��'			then '������ �� ���'
													when '��������� �. �. ��'	then '��������� �.�. ��'
													when '���� ��'				then '���� ���'
													when '�������� �. �. ��'	then '�������� �.�. ��'
													when '����������� �������'	then '����������� ������� ���'
													when '�� �����'				then '�� ����� ���'
													when '�������� ��� ����'	then '�������� ��� ���� ���'

													else trim(shipment_customer_name)
												 end;




			-- ��� � ��������� ��������
			update ts
			set ts.shipment_priority = c.shipment_priority
			   ,ts.shipment_min_KOS	 = c.manual_KOS
			from project_plan_production_finished_products.data_import.shipments_sales_plan as ts
			join project_plan_production_finished_products.info.customers as c
				on ts.shipment_customer_id = c.customer_id
				and ts.shipment_sales_channel_name = c.sales_channel_name
			where not c.shipment_priority is null 
				and not c.manual_KOS is null;


			-- ��������� ������� � ����� ����� ���� ��� � project_plan_production_finished_products.info.customers
			insert into project_plan_production_finished_products.info.customers
			(
					 customer_id
					,customer_name
					,sales_channel_name	
					,source_insert
			)
			select 
					 sp.shipment_customer_id			
					,min(sp.shipment_customer_name)
					,sp.shipment_sales_channel_name	
					,'���� ������ �� ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert			
			from project_plan_production_finished_products.data_import.shipments_sales_plan as sp
			join project_plan_production_finished_products.data_import.info_excel as ie on sp.name_table = ie.name_table
			where not sp.shipment_customer_id is null
			  and not sp.shipment_sales_channel_name is null
			  and not exists (select * 
							  from project_plan_production_finished_products.info.customers as c
							  where sp.shipment_customer_id = c. customer_id
							    and sp.shipment_sales_channel_name = c.sales_channel_name)	
			group by 
					 sp.shipment_customer_id
					,sp.shipment_sales_channel_name;	
				

			-- ��������� ����������
			update c
			set	 c.dt_tm_change = getdate()
				,c.source_insert = d.source_insert	
			from project_plan_production_finished_products.info.customers as c
			join (
					select
							 d.shipment_customer_id	
							,d.shipment_customer_name
							,d.shipment_sales_channel_name	
							,'���� ������ �� ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert
					from project_plan_production_finished_products.data_import.shipments_sales_plan as d
					join project_plan_production_finished_products.data_import.info_excel as ie on d.name_table = ie.name_table
					where not d.shipment_customer_id is null
						and not d.shipment_sales_channel_name is null	
					group by 
							 d.shipment_customer_id	
							,d.shipment_customer_name
							,d.shipment_sales_channel_name	
					) as d on c.customer_id = d.shipment_customer_id
						and c.sales_channel_name = d.shipment_sales_channel_name;



			-- ������� ���� �������� c �������
			update c
			set c.shipment_date = DATEADD(day, -b.to_branch_days, c.shipment_with_branch_date)
			from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			join project_plan_production_finished_products.info.branches as b
				on c.shipment_branch_id = b.branch_id;


			
			-- ������� �������� ����� ���� �������� ������ 
			select @dt_for_delete = max(date_file) + 1
			from project_plan_production_finished_products.data_import.info_excel 
			where name_table in ('shipments_SAP', 'shipments_1C');
	
			delete project_plan_production_finished_products.data_import.shipments_sales_plan
			where not shipment_date is null
			  and shipment_date <= @dt_for_delete;



			
			-- ����������� SAP ID � ������ ���� ������, article_packaging ������ ���� 1
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			select *, count(s.sap_id) over (partition by s.article_packaging) as check_double_sap_id
			into #sap_id
			from (
					select distinct
							 s1.article_packaging
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
			set c.sap_id							= s.SAP_id
				,c.stuffing_id						= s.stuffing_id
				,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
				,c.product_status					= s.product_status
			from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			join #sap_id as s on c.article_packaging = s.article_packaging
			where s.check_double_sap_id = 1;


			-- ��������� ��������� �� �������
			begin

					insert into project_plan_production_finished_products.data_import.shipments_sales_plan
					(
							 reason_ignore_in_calculate		
							,product_status
							,sap_id							
							,sap_id_expiration_date_in_days	
							
							,stuffing_id
							,stuffing_id_box_row_id
							,stuffing_id_box		

							,sap_id_from_excel
							,position_dependent_id
							,individual_marking_id
							,article_nomenclature
							,article_packaging
							,product_finished_id

							,shipment_promo_status
							,shipment_promo
							,shipment_promo_kos_listing

							,shipment_sales_channel_name
							,shipment_branch_id
							,shipment_branch_name
							,shipment_customer_id
							,shipment_customer_name
							,shipment_priority
							,shipment_min_KOS
							,shipment_with_branch_date
							,shipment_date
							,shipment_kg 

					)

					select	
							 s.reason_ignore_in_calculate		
							,s.product_status
							,s.sap_id						
							,s.sap_id_expiration_date_in_days	
							
							,t.stuffing_id
							,s.row_id as stuffing_id_box_row_id
							,t.stuffing_id_box		

							,s.sap_id_from_excel
							,s.position_dependent_id
							,s.individual_marking_id
							,s.article_nomenclature
							,s.article_packaging
							,s.product_finished_id

							,s.shipment_promo_status
							,s.shipment_promo
							,s.shipment_promo_kos_listing

							,s.shipment_sales_channel_name
							,s.shipment_branch_id
							,s.shipment_branch_name
							,s.shipment_customer_id
							,s.shipment_customer_name
							,s.shipment_priority
							,s.shipment_min_KOS
							,s.shipment_with_branch_date
							,s.shipment_date
							,s.shipment_kg * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg

					from project_plan_production_finished_products.data_import.shipments_sales_plan as s
					join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;
					

					
					--����������� row_id � ������ �������
					update s
					set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
					from project_plan_production_finished_products.data_import.shipments_sales_plan as s
					join (select distinct stuffing_id_box_row_id
						  from project_plan_production_finished_products.data_import.shipments_sales_plan 
						  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
					-- ����������� ��� �������
					update project_plan_production_finished_products.data_import.shipments_sales_plan
					set stuffing_id_box_type = case 
													when stuffing_id_box_row_id is null then 0 -- ������� �� �������
													when stuffing_id_box is null		then 1 -- ������� �������
													when not stuffing_id_box is null	then 2 -- ������� �������� �� �������
											   end;
			end;



			---- ����� ������ ---------------------------------------------------------------
			update d
			Set reason_ignore_in_calculate = 
				nullif(
							case 
								when (select top 1 c.check_double_sap_id from #sap_id as c where d.article_packaging = c.article_packaging)>1 
																				then	'������� ���� ��������� > 1 SAP ID | '
								when d.sap_id is null							then	'�� ������ sap id | '
								when d.stuffing_id is null						then	'��� ������� ����������� | '
								when d.sap_id_expiration_date_in_days is null	then	'����������� ���� �������� | '
								else ''
							end
						+ iif(shipment_min_KOS is null,						'����������� ��� | ', '')
						+ iif(isnull(shipment_sales_channel_name, '') = '',	'����� ����� �� �������� | ', '')
						+ iif(shipment_with_branch_date is null,			'���� �������� �����������  | ', '')
						, '')
			from project_plan_production_finished_products.data_import.shipments_sales_plan as d;

			-- ��������� ������ � excel
			select 

					 '������'							= h.reason_ignore_in_calculate
					,'������ ���������� SKU'			= h.product_status
					,'SAP ID'							= h.sap_id_text
					,'��� �������'						= h.stuffing_id

					,'��� ��������� �������'			= h.position_dependent_id
					,'��� �������������� ����������'	= h.individual_marking_id
					,'������� ����'						= h.article_packaging
					,'������� ������������'				= h.article_nomenclature

					,'��� �������'						= h.shipment_branch_id	
					,'�������� �������'					= h.shipment_branch_name
					,'�������� ������ �����'			= h.shipment_sales_channel_name
					,'��� �����������'					= h.shipment_customer_id
					,'�������� �����������'				= h.shipment_customer_name
					,'��������� ��������'				= h.shipment_priority
					,'��� ��������'						= h.shipment_min_KOS
					,'���� �������� � �������'			= h.shipment_with_branch_date
					,'���� ��������'					= h.shipment_date
					,'���� ������, ��'					= h.shipment_kg

					,'����/��� �����'					= ie.path_file
					,'������ �� ����'					= ie.date_file
					,'��� ��������'						= ie.user_insert
					,'���� � ����� ��������'			= ie.dt_tm_insert

			from project_plan_production_finished_products.data_import.shipments_sales_plan as h
			join project_plan_production_finished_products.data_import.info_excel as ie on h.name_table = ie.name_table
			where h.stuffing_id_box_type in (0, 1);
		


					
end;