/********************************/
-- Канал сбыты
-- Автор: Рощин МЮ 
-- Версия: 20190925
/********************************/
sales channels
select * from cherkizovo.info.sales_channels

drop table cherkizovo.info.sales_channels

create table cherkizovo.info.sales_channels
(
	 sales_channel_id		smallint	not null
	,sales_channel_name		varchar(50)	not null	
	 CONSTRAINT [PK sales_channels | sales_channel_id] PRIMARY KEY CLUSTERED (sales_channel_id)
)

insert into cherkizovo.info.sales_channels
(sales_channel_id, sales_channel_name)
select 10, 'Внутрихолдинговый'		union all
select 11, 'Сети'					union all
select 12, 'Розница'				union all
select 13, 'HoReCa'					union all
select 14, 'Опт'					union all
select 15, 'Экспорт'				union all
select 16, 'Прочие продажи'			union all
select 17, 'Дистрибьютор'			union all
select 98, 'Фирменная розница'		union all
select 99, 'Не указано'				



