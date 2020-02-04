

DECLARE @json NVARCHAR(MAX)
SET @json =  
N'[  
	{ "data_type" : "log остатки"
	 ,"log_from_row_id": 1 '+'
	 ,"log_for_row_id": 2
	 ,"log_kg": 2.0 }
  ]'  
   
SELECT *  
FROM OPENJSON(@json)  
  WITH (data_type			varchar(50) '$.data_type'  
        ,log_from_row_id	int			'$.log_from_row_id'
		,log_for_row_id		int			'$.log_for_row_id'  
        ,age				dec(11,5)	'$.log_kg')
		
		




							DECLARE @json NVARCHAR(MAX)
SET @json =  
N'[  
       { "id" : 2,"info": { "name": "John", "surname": "Smith" }, "age": 25 },  
       { "id" : 5,"info": { "name": "Jane", "surname": "Smith" }, "dob": "2005-11-04T12:00:00" }  
 ]'  
   
SELECT *  
FROM OPENJSON(@json)  
  WITH (id int 'strict $.id',  
        firstName nvarchar(50) '$.info.name', lastName nvarchar(50) '$.info.surname',  
        age int, dateOfBirth datetime2 '$.dob')  




							SELECT *  
					FROM OPENJSON(@json_log)  
					  WITH ( data_type		varchar(50) '$.data_type'  
							,from_row_id	int			'$.from_row_id'
							,for_row_id		int			'$.for_row_id'  
							,kg				dec(11,5)	'$.kg' );