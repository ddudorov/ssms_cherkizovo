use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments @shipment_data_type = 'shipments_1C'

go

alter procedure check_import.shipments @shipment_data_type VARCHAR(30)	
as
BEGIN
			SET NOCOUNT ON;


			begin -- УДАЛЯЕМ: АДРЕСА ДОСТАВКИ, ТАК КАК ЭТО ВНУТР ПЕРЕМЕЩЕНИЯ 

					delete 
					from project_plan_production_finished_products.data_import.shipments
					where (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Москва г, Пермская ул, вл. 5'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in (', Москва г, Пермская ул., дом № 5'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('115372, Москва г, Бирюлевская ул., дом № 38'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
					   or (shipment_customer_name in ('ЧМПЗ АО')		  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
					   or (isnull(shipment_kg, 0) = 0);

			end;

			if @shipment_data_type = 'shipments_sales_plan' -- В ПЛАНЕ ПРОДАЖ: СЧИТАЕМ ДАТУ ОТГРУЗКИ C ФИЛИАЛА / УДАЛЯЕМ ОТГРУЗКИ ПОСЛЕ ДАТЫ ОТГРУЗКИ ЗАЯВОК 
			begin  

						-- СЧИТАЕМ ДАТУ ОТГРУЗКИ C ФИЛИАЛА
						update c
						set c.shipment_date = DATEADD(day, -b.to_branch_days, c.shipment_with_branch_date)
						from project_plan_production_finished_products.data_import.shipments as c
						join project_plan_production_finished_products.info.branches as b on c.shipment_branch_id = b.branch_id and not c.shipment_branch_id is null;
				
						-- УДАЛЯЕМ ОТГРУЗКИ ПОСЛЕ ДАТЫ ОТГРУЗКИ ЗАЯВОК 
						delete s
						from project_plan_production_finished_products.data_import.shipments as s
						join (
								select max(data_on_date) + 1  as shipment_date
								from project_plan_production_finished_products.data_import.data_type
								where data_type in ('shipments_SAP', 'shipments_1C')
							 ) as d on s.shipment_data_type	= 'shipments_sales_plan' and s.shipment_date <= d.shipment_date;

			end; 
		
			

			select * from project_plan_production_finished_products.data_import.shipments











end;










