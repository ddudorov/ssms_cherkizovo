ОСГ006_1-30 дней
ОСГ007_31-35 дней
ОСГ008_36-45 дней
ОСГ009_46-60 дней
ОСГ010_61-90 дней
ОСГ011_91-365 дней
ОСГ012_366-999 дней

-- это срок годности для SKU, у SKU есть срок годности, например 45 дней, значит категория будет 8
drop table cherkizovo.information.[Категория ОСГ]
create table cherkizovo.information.[Категория ОСГ]
(
	 [ИД Категория ОСГ]	tinyint not null
	,[дней от]			smallint not null
	,[дней до]			smallint not null	
	CONSTRAINT [ИД Категория ОСГ] PRIMARY KEY CLUSTERED ([ИД Категория ОСГ]) 
)


insert into cherkizovo.information.[Категория ОСГ]
values(6,	1,		30)
	 ,(7,	31,		35)
	 ,(8,	36,		45)
	 ,(9,	46,		60)
	 ,(10,	61,		90)
	 ,(11,	91,		365)
	 ,(12,	366,	999)

select * from cherkizovo.information.[Категория ОСГ]
