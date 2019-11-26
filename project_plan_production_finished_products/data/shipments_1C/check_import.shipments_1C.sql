use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments_1C

go

alter procedure check_import.shipments_1C								
as
BEGIN
			SET NOCOUNT ON;

			-- �������, ������ ��������, ��� ��� ��� ����� �����������
			delete 
			from project_plan_production_finished_products.data_import.shipments_1C 
			where shipment_customer_name	in ('�� ��������� ���') 
			  and shipment_delivery_address in ( '107143, ������ �, �������� ��, ��. 5'
												,', ������ �, �������� ��., ��� � 5'
												,'115372, ������ �, ����������� ��., ��� � 38'
												,'107143, ������, ������ �, �������� ��; ��. 5');

			delete 
			from project_plan_production_finished_products.data_import.shipments_1C 
			where shipment_customer_name	in ('���� ��')		  
			  and shipment_delivery_address in ('107143, ������, ������ �, �������� ��; ��. 5');



			-- ����������� SAP ID � ������ 1�, article_packaging ������ ���� 1
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
			from project_plan_production_finished_products.data_import.shipments_1C as c
			join #sap_id as s on c.article_packaging = s.article_packaging
			where s.check_double_sap_id = 1;


			-- ����������� ������ � 1C
			update c
			set c.shipment_sales_channel_name = r.[�������� ������ �����]
				,c.shipment_priority = r.[��������� ��������]
				,c.shipment_min_KOS = r.[������ ���]
			from project_plan_production_finished_products.data_import.shipments_1C as c
			join project_plan_production_finished_products.info_view.customers as r 
				on c.shipment_customer_name = r.[�������� �����������]
				and not r.[������] like '%�������� ����������� �����������%'





			-- ��������� ��������� �� �������
			begin

					insert into project_plan_production_finished_products.data_import.shipments_1C
					(
							 reason_ignore_in_calculate
							,product_status
							,sap_id
							,sap_id_expiration_date_in_days
							,stuffing_id
							,stuffing_id_box_row_id
							,stuffing_id_box
							,article_packaging
							,shipment_sales_channel_name
							,shipment_customer_name
							,shipment_delivery_address
							,shipment_priority
							,shipment_min_KOS
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
							,s.article_packaging
							,s.shipment_sales_channel_name
							,s.shipment_customer_name
							,s.shipment_delivery_address
							,s.shipment_priority
							,s.shipment_min_KOS
							,s.shipment_date
							,s.shipment_kg
							 * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg
					from project_plan_production_finished_products.data_import.shipments_1C as s
					join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;

					
					-- ����������� row_id � ������ �������
					update s
					set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
					from project_plan_production_finished_products.data_import.shipments_1C as s
					join (select distinct stuffing_id_box_row_id
						  from project_plan_production_finished_products.data_import.shipments_1C 
						  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
					-- ����������� ��� �������
					update project_plan_production_finished_products.data_import.shipments_1C
					set stuffing_id_box_type = case 
													when stuffing_id_box_row_id is null then 0 -- ������� �� �������
													when stuffing_id_box is null		then 1 -- ������� �������
													when not stuffing_id_box is null	then 2 -- ������� �������� �� �������
											   end;

			end;


			---- ����� ������ ---------------------------------------------------------------
			update d
			Set d.reason_ignore_in_calculate = 
				nullif(
							case 
								when (select top 1 c.check_double_sap_id from #sap_id as c where d.article_packaging = c.article_packaging) > 1 
																				then	'������� ���� ��������� > 1 SAP ID | '
								when d.sap_id is null							then	'�� ������ sap id | '
								when d.stuffing_id is null						then	'��� ������� ����������� | '
								when d.sap_id_expiration_date_in_days is null	then	'����������� ���� �������� | '
								else ''
							end
						+ iif(d.shipment_sales_channel_name is null,					'����� ����� �� �������� | ', '')
						+ iif(d.shipment_priority is null,								'����������� ��������� �������� | ', '')
						+ iif(d.shipment_min_KOS is null,								'����������� ��� | ', '')
						, '')
			from project_plan_production_finished_products.data_import.shipments_1C as d;



			

			-- ������� ����� ��������� �������
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;


			-- ��������� ������ � excel
			select 
					 h.reason_ignore_in_calculate
					,h.product_status
					,h.sap_id_text
					,h.stuffing_id
					,h.article_packaging
					,h.shipment_sales_channel_name
					,h.shipment_customer_name
					,h.shipment_delivery_address
					,h.shipment_priority
					,h.shipment_min_KOS
					,h.shipment_date
					,h.shipment_kg
					,ie.path_file
					,ie.date_file
					,ie.user_insert
					,ie.dt_tm_insert
			from project_plan_production_finished_products.data_import.shipments_1C as h
			join project_plan_production_finished_products.data_import.info_excel as ie on h.name_table = ie.name_table
			where h.stuffing_id_box_type in (0, 1);

end;


