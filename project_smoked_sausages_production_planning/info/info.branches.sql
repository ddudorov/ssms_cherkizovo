use project_plan_production_finished_products;
--use project_plan_production_finished_products_development

go

CREATE TABLE info.branches
(
		 branch_id			varchar(20)		not null	
		,branch_name		varchar(100)	not null
		,to_branch_days		tinyint			not null	
		CONSTRAINT [PK branchs | branch_id]	PRIMARY KEY CLUSTERED (branch_id) 
);


ALTER VIEW info_view.branches
AS
	SELECT 
		   'Дейcтвие'				= ''
		  ,'Код филиала'			= branch_id 
		  ,'Название филиала'		= branch_name 
		  ,'Доставка от ТДЧ, дней'	= to_branch_days 
	FROM info.branches;




select *
from info.branches


