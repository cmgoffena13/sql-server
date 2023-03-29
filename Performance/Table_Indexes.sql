USE {database_name}; 
GO

/*====================================================================================================
Table Indexes - Cortland Goffena

Shows all indexes on table and corresponding information such as usage, size, and object definition
Note: stats may be reset by some sql server backend actions, as well as a restart
====================================================================================================*/

DECLARE @TableName VARCHAR(100) = 'schema.tablename'


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DROP TABLE IF EXISTS #Indexes_Initial
DROP TABLE IF EXISTS #Indexes_Prep
DROP TABLE IF EXISTS #Indexes_PK
DROP TABLE IF EXISTS #Indexes_Agg
DROP TABLE IF EXISTS #Indexes_Definition


SELECT
CONCAT( SCHEMA_NAME ( t.schema_id ), '.', t.name ) AS TableName,
i.name AS IndexName,
i.type_desc AS IndexType,
CASE
    WHEN ic.is_descending_key = 1 THEN col.name + ' DESC'
    ELSE col.name
END AS ColumnName,
i.is_unique,
i.is_primary_key,
i.is_unique_constraint,
ic.key_ordinal,
ic.is_included_column,
col.is_nullable,
i.filter_definition
INTO #Indexes_Initial
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = i.object_id
    AND ic.index_id = i.index_id
INNER JOIN sys.columns AS col
    ON col.object_id = ic.object_id
    AND col.column_id = ic.column_id
INNER JOIN sys.tables t
    ON i.object_id = t.object_id
WHERE t.type_desc = 'USER_TABLE'
AND CONCAT( SCHEMA_NAME( t.schema_id ), '.', t.name ) = @TableName


SELECT 
TableName,
IndexName,
CASE
    WHEN IndexType = 'NONCLUSTERED' THEN 1
    ELSE 0
END AS NonCluster,
CASE
    WHEN IndexType = 'CLUSTERED' AND is_included_column = 0 THEN ColumnName
END AS PrimaryKeyColumn,
CASE
    WHEN IndexType = 'NONCLUSTERED' AND is_included_column = 0 AND is_unique = 0 THEN ColumnName
END AS NonUniqueKeyColumn,
CASE
    WHEN IndexType = 'NONCLUSTERED' AND is_included_column = 0 AND is_unique = 1 THEN ColumnName
END AS UniqueKeyColumn,
CASE
    WHEN is_included_column = 1 THEN ColumnName
END AS IncludeColumn,
CASE
    WHEN is_nullable = 1 THEN 1
    ELSE 0
END AS Nullable,
filter_definition,
key_ordinal
INTO #Indexes_Prep
FROM #Indexes_Initial


SELECT 
TableName, 
STRING_AGG ( PrimaryKeyColumn, ', ' ) WITHIN GROUP( ORDER BY key_ordinal ) AS PrimaryKeys
INTO #Indexes_PK
FROM #Indexes_Prep
GROUP BY TableName


SELECT 
p.TableName,
p.IndexName,
pk.PrimaryKeys,
STRING_AGG ( p.UniqueKeyColumn, ', ' ) WITHIN GROUP( ORDER BY p.key_ordinal ) AS UniqueKeys,
STRING_AGG ( p.NonUniqueKeyColumn, ', ' ) WITHIN GROUP( ORDER BY p.key_ordinal ) AS NonUniqueKeys,
STRING_AGG ( p.IncludeColumn, ', ' ) AS Includes,
p.filter_definition AS FilterDefinition
INTO #Indexes_Agg
FROM #Indexes_Prep AS p
INNER JOIN #Indexes_PK AS pk
    ON pk.TableName = p.TableName
GROUP BY 
p.TableName, 
p.IndexName, 
pk.PrimaryKeys, 
p.filter_definition


SELECT 
TableName,
IndexName,
CONCAT (
    'PK (' + PrimaryKeys + ');',
    ' UNIQUE KEYS (' + UniqueKeys + ');',
    ' KEYS (' + NonUniqueKeys + ');',
    ' INCLUDE (' + Includes + '); ',
    ', WHERE ' + FilterDefinition
) AS IndexDefinition
INTO #Indexes_Definition
FROM #Indexes_Agg;


SELECT
CAST( 
    ( SELECT DATEDIFF( DAY, sqlserver_start_time, GETDATE() ) AS DaysSinceLastRestart FROM sys.dm_os_sys_info ) AS VARCHAR(20)
    ) + ' Days Ago' AS LastRestart,
CONCAT( SCHEMA_NAME( t.schema_id ), '.', t.name ) AS TableName,
CASE 
    WHEN LEN( ps.row_count ) > 9 THEN 
        CAST( CAST( LEFT( ps.row_count, LEN( ps.row_count ) - 9) AS VARCHAR(30) ) + ' Billion Records' AS VARCHAR(30))
    WHEN LEN( ps.row_count ) > 6 THEN 
        CAST( CAST( LEFT( ps.row_count, LEN( ps.row_count ) - 6) AS VARCHAR(30) ) + ' Million Records' AS VARCHAR(30))
    WHEN LEN( ps.row_count ) > 3 THEN 
        CAST( CAST( LEFT( ps.row_count, LEN( ps.row_count ) - 3) AS VARCHAR(30) ) + ' Thousand Records' AS VARCHAR(30))
    ELSE CAST( CAST( ps.row_count AS VARCHAR(30) ) + ' Records' AS VARCHAR(30) )
END  AS Records,
ISNULL( i.name, 'HEAP' ) AS IndexName,
ISNULL( id.IndexDefinition, 'HEAP' ) AS IndexDefinition,
i.type_desc AS TypeDescription,
CAST( 
    CAST(
        CAST( ( ps.used_page_count * 8 ) AS DECIMAL(17,2) )
         / 1024 AS DECIMAL(17,2) )
 / 1024 AS DECIMAL(17,2) ) AS Gigabytes,
CASE
    WHEN ISNULL ( iu.user_seeks, 0 ) + ISNULL ( iu.user_scans, 0 ) + ISNULL ( iu.user_lookups, 0 ) = 0 THEN 0
    ELSE 1
END AS IndexUsed,
CASE
    WHEN iu.user_scans > 0 AND iu.user_seeks = 0 THEN 1
    ELSE 0
END AS ScanOnly,
ISNULL ( iu.user_seeks, 0 ) AS Seeks,
ISNULL ( iu.user_scans, 0 ) AS Scans,
ISNULL ( iu.user_lookups, 0 ) AS Lookups,
ISNULL ( iu.user_seeks, 0 ) + ISNULL ( iu.user_scans, 0 ) + ISNULL ( iu.user_lookups, 0 ) AS TotalOperations,
iu.last_user_seek AS LastSeek,
iu.last_user_scan AS LastScan,
iu.last_user_lookup AS LastLookup,
DATEDIFF ( MINUTE, iu.last_user_update, GETDATE () ) AS MinutesSinceLastUpdate,
iu.last_user_update AS LastIndexUpdate,
(
    SELECT sqlserver_start_time AS LastRestart FROM sys.dm_os_sys_info
) AS LastRestartTime
FROM sys.tables AS t
INNER JOIN sys.indexes AS i
    ON i.object_id = t.object_id
LEFT JOIN sys.dm_db_index_usage_stats AS iu
    ON iu.object_id = i.object_id
    AND iu.index_id = i.index_id
LEFT JOIN sys.dm_db_partition_stats AS ps
    ON ps.object_id = i.object_id
    AND ps.index_id = i.index_id
LEFT JOIN #Indexes_Definition AS id
    ON id.TableName = ( CONCAT( SCHEMA_NAME ( t.schema_id ), '.', t.name ) )
    AND id.IndexName = i.name
WHERE CONCAT( SCHEMA_NAME( t.schema_id ), '.', t.name ) = @TableName
    AND ISNULL( iu.database_id, DB_ID() ) = DB_ID()
ORDER BY ps.used_page_count DESC; 