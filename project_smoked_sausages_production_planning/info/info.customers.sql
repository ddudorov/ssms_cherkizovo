/***************/
/*** набивка ***/
/***************/
use project_plan_production_finished_products_development
--use project_plan_production_finished_products

-- таблица
select * from info.customers
-- drop table info.customers

create table info.customers
(
	 customer_id				varchar(20)		NOT NULL
	,customer_name				varchar(100)	NOT NULL	
	,sales_channel_name			varchar(25)		NOT NULL
	,shipment_priority			tinyint				NULL	-- Приоритет отгрузки\
	,manual_shipment_priority	tinyint				NULL
	,SAP_min_KOS				decimal(3, 2)		NULL	-- КОС % минимальный
	,SAP_max_KOS				decimal(3, 2)		NULL	-- КОС % максимальный
	,manual_KOS					decimal(3, 2)		NULL	-- Срок действия КОС от	
	,dt_tm_change				datetime		not null	default getdate()
	,source_insert				varchar(100)		null
	 CONSTRAINT [PK customers | customer_id, sales_channel_id] PRIMARY KEY(customer_id, sales_channel_name) 
)

alter table  info.customers
add type_channel_name			varchar(25)		NULL

alter table  info.customers
drop column type_channel_name			varchar(25)		 NULL


-- набивка
alter VIEW info_view.customers
as
	select 
			 'Дейcтвие'					= ''
			,'Код контрагента'			= c.customer_id			
			,'Название контрагента'		= c.customer_name	
			,'Название канала сбыта'	= c.sales_channel_name
			,'Канал'					= c.type_channel_name
			,'Приор отгрузки'			= c.shipment_priority		
			,'Ручной приор отгрузки'	= c.manual_shipment_priority	
			,'SAP min КОС'				= c.SAP_min_KOS
			,'SAP max КОС'				= c.SAP_max_KOS
			,'Ручной КОС'				= c.manual_KOS	
			,'Ошибки' = nullif(
								case when count(1) over (partition by c.customer_name) > 1 
									 then 'Название контрагента дублируется: ' + convert(varchar(5), count(1) over (partition by c.customer_name)) + ' | ' else '' end +
								case when c.shipment_priority is null 
									 then 'Отсутствует приоритет отгрузки | ' else '' end +
								case when c.manual_KOS is null 
									 then 'Отсутствует ручной КОС | ' else '' end +
								case when not c.SAP_min_KOS is null and not c.SAP_max_KOS is null and not c.manual_KOS is null and not c.manual_KOS	between c.SAP_min_KOS and c.SAP_max_KOS
									 then 'Ручной КОС выше или ниже КОС SAP | ' else '' end
							 , '')
			,'Дата добавления' = c.dt_tm_change
			,'Источник' = c.source_insert
	from info.customers as c;


select *
from info_view.customers