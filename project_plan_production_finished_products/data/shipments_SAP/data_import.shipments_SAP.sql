use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.shipments_SAP
--drop table project_plan_production_finished_products.data_import.shipments_SAP
create table project_plan_production_finished_products.data_import.shipments_SAP
(

		 row_id							INT					NOT NULL	IDENTITY(1, 1)
		,name_table						as 'shipments_SAP'
		,reason_ignore_in_calculate		VARCHAR(300)			NULL
		,product_status					varchar(100)			NULL

		,sap_id							BIGINT					NULL
		,sap_id_text					as convert(varchar(24), FORMAT(sap_id, '000000000000000000000000'))
		,sap_id_expiration_date_in_days	SMALLINT				NULL

		,stuffing_id					VARCHAR(40)				NULL
		,stuffing_id_box_type			TINYINT					NULL
		,stuffing_id_box_row_id			INT						NULL
		,stuffing_id_box				VARCHAR(40)				NULL

		,position_dependent_id			INT						NULL	
		,individual_marking_id			TINYINT					NULL	

		-- ������
		,shipment_delete				tinyint					NOT NULL default 0
		,shipment_sales_channel_id		TINYINT					NULL
		,shipment_sales_channel_name	as	case shipment_sales_channel_id
											when 10 then '�����������������'
											when 11 then '����'
											when 12 then '�������'
											when 13 then 'HoReCa'
											when 14 then '���'
											when 15 then '�������'
											when 16 then '������ �������'
											when 17 then '������������'
											--when 98 then '��������� �������'
											--when 99 then '�� �������'										
											end	
												
		,shipment_customer_id			VARCHAR(20)				NULL
		,shipment_customer_name			VARCHAR(100)			NULL
		,shipment_delivery_address		VARCHAR(1000)			NULL

		,shipment_priority				TINYINT					NULL
		,shipment_min_KOS				DEC(7,6)				NULL
		
		,shipment_date					DATETIME				NULL
		,shipment_kg					dec(11,5)				NULL

		-- ��������� ����
		,stock_shipment_kg				dec(11,5)				NULL
		,stock_net_need_kg				as nullif( shipment_kg - isnull(stock_shipment_kg, 0)   , 0)
		   
		,stuffing_fact_shipment_kg		dec(11,5)				NULL	-- ���-�� ������� ��� ��������� �� ������� ������� ����������
		,stuffing_fact_net_need_kg		as nullif( shipment_kg - isnull(stock_shipment_kg, 0) - isnull(stuffing_fact_shipment_kg, 0)   , 0)
		   
		,stuffing_plan_shipment_kg		dec(11,5)				NULL	-- ���-�� ������� ��� ��������� �� ������� ������� ����������
		,stuffing_plan_net_need_kg		as nullif( shipment_kg - isnull(stock_shipment_kg, 0) - isnull(stuffing_fact_shipment_kg, 0) - isnull(stuffing_plan_shipment_kg, 0)   , 0)
		
		,marking_shipment_kg			dec(11,5)				NULL
		,marking_net_need_kg			as nullif( shipment_kg - isnull(stock_shipment_kg, 0) - isnull(marking_shipment_kg, 0)   , 0)
)












