USE {database_name};
GO


DECLARE @TableName sysname = 'schema.name';


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @ColumnList VARCHAR(MAX), @MBList VARCHAR(MAX);


SELECT @ColumnList = STRING_AGG ( c.name, ', ' ) WITHIN GROUP(ORDER BY c.name),
       @MBList = STRING_AGG (
                            CONCAT ( 'SUM(CAST(DATALENGTH(', c.name, ') AS BIGINT)) / (1024*1024) AS ', c.name ), ', '
                            ) WITHIN GROUP(ORDER BY c.name)
FROM sys.tables AS o
INNER JOIN sys.columns AS c
    ON c.object_id = o.object_id
INNER JOIN sys.types AS t
    ON t.user_type_id = c.user_type_id
WHERE o.object_id = OBJECT_ID ( @TableName );


DECLARE @SQL NVARCHAR(MAX);

SET @SQL = N'
	SELECT
	@TableName AS TableName,
	{mb_list}
	INTO #Lengths
	FROM ' + @TableName + N'';

SET @SQL = REPLACE ( @SQL, '{mb_list}', @MBList );

SET @SQL += N'

	SELECT c.name AS ColumnName,
       CASE
           WHEN t.name = ''decimal'' THEN CONCAT ( t.name, '' ('', c.precision, '','', c.scale, '')'' )
           WHEN t.name = ''datetimeoffset'' THEN CONCAT ( t.name, '' ('', c.scale, '')'' )
           WHEN t.name IN ( ''char'', ''varchar'' ) THEN CONCAT ( t.name, '' ('', c.max_length, '')'' )
           WHEN t.name IN (''nvarchar'', ''nchar'') THEN CONCAT ( t.name, '' ('', c.max_length / 2, '')'' )
           WHEN t.name = ''varbinary'' THEN CONCAT ( t.name, '' ('', c.max_length, '')'' )
		   ELSE t.name
       END AS DataType,
       c.is_nullable AS Nullable
	INTO #Mapping
	FROM sys.tables AS o
	INNER JOIN sys.columns AS c
		ON c.object_id = o.object_id
	INNER JOIN sys.types AS t
		ON t.user_type_id = c.user_type_id
	WHERE o.object_id = OBJECT_ID ( @TableName );
';

SET @SQL += N'

	SELECT
	TableName,
	ColumnName,
	SpaceUsedMB
	INTO #Final
	FROM (
	SELECT
	TableName,
	{column_list}
	FROM #Lengths) AS Subquery
	UNPIVOT
	( SpaceUsedMB FOR ColumnName IN (
	{column_list}
	)) AS pvt';

SET @SQL = REPLACE ( @SQL, '{column_list}', @ColumnList );

SET @SQL += N'

	SELECT
	F.TableName,
	F.ColumnName,
	M.DataType,
	M.Nullable,
	F.SpaceUsedMB,
	CAST(CAST(F.SpaceUsedMB AS DECIMAL(17,2)) / 1024 AS DECIMAL(17,2)) AS SpaceUsedGB
	FROM #Final AS F
	INNER JOIN #Mapping AS M
		ON M.ColumnName = F.ColumnName
	ORDER BY F.SpaceUsedMB DESC';

--PRINT @SQL;
EXEC sys.sp_executesql @SQL, N'@TableName VARCHAR(150)', @TableName = @TableName;