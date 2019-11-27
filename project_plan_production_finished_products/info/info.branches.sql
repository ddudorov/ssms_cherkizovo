use project_plan_production_finished_products;

go

CREATE TABLE project_plan_production_finished_products.info.branches
(
		 branch_id			varchar(20)		not null	
		,branch_name		varchar(100)	not null
		,to_branch_days		tinyint			not null	
		CONSTRAINT [PK branchs | branch_id]	PRIMARY KEY CLUSTERED (branch_id) 
);


CREATE VIEW info_view.branches
AS
	SELECT branch_id as 'Код филиала'
		  ,branch_name as 'Название филиала'
		  ,to_branch_days  as 'Доставка от ТДЧ, дней'
	FROM project_plan_production_finished_products.info.branches




select *
from project_plan_production_finished_products.info.branches

select *
from project_plan_production_finished_products.info_view.branches

