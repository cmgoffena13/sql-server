USE {database_name};
GO


DECLARE @Top INT = 25;


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
WITH CTE AS (
    SELECT
    t.name AS TableName,
    t.schema_id,
    i.index_id,
    i.name AS IndexName,
    SUM( p.rows ) AS RecordCount,
    SUM( a.total_pages ) AS TotalPages,
    SUM( a.used_pages ) AS UsedPages,
    SUM( a.data_pages ) AS DataPages,
    SUM( a.total_pages / 128 ) AS TotalSpaceMB,
    SUM( a.used_pages / 128 ) AS UsedSpaceMB,
    SUM( a.data_pages / 128 ) AS DataSpaceMB,
    SUM( CASE WHEN i.type_desc = 'NONCLUSTERED' THEN a.total_pages / 128 ELSE 0 END ) AS TotalNonclusteredSpaceMB,
    SUM( CASE WHEN i.type_desc = 'CLUSTERED' OR i.type_desc = 'HEAP' THEN a.total_pages / 128 ELSE 0 END ) AS TotalTableSpaceMB
    FROM sys.tables t
    INNER JOIN sys.indexes i
        ON t.object_id = i.object_id
    INNER JOIN sys.partitions p
        ON i.object_id = p.object_id
        AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a
        ON p.partition_id = a.container_id
    WHERE t.is_ms_shipped = 0
    GROUP BY
        t.name,
        t.schema_id,
        i.name,
        i.index_id,
        i.object_id
),
CTE2 AS (
    SELECT
    SCHEMA_NAME( schema_id ) + '.' + TableName AS TableName,
    COUNT( DISTINCT index_id ) AS IndexCount,
    MAX( RecordCount ) AS RecordCount,
    CAST( SUM( CAST( TotalSpaceMB AS DECIMAL( 17, 2 ) ) ) / 1024 AS DECIMAL( 17, 2 ) ) AS TotalSpaceGB,
    SUM( TotalPages ) * 8 AS TotalSpaceKB,
    CAST( SUM( CAST( TotalNonclusteredSpaceMB AS DECIMAL( 17, 2 ) ) ) / 1024 AS DECIMAL( 17, 2 ) ) AS IndexSpaceGB,
    CAST( SUM( CAST( TotalTableSpaceMB AS DECIMAL( 17, 2 ) ) ) / 1024 AS DECIMAL( 17, 2 ) ) AS TableSpaceGB
    FROM CTE
    GROUP BY SCHEMA_NAME( schema_id ) + '.' + TableName
)

SELECT TOP ( @Top )
TableName
TotalSpaceGB,
TableSpaceGB,
IndexCount,
IndexSpaceGB,
ISNULL ( CAST( CAST( IndexSpaceGB AS DECIMAL(17, 2) )
    / NULLIF( CAST( TotalSpaceGB AS DECIMAL(17, 2) ), 0 ) AS DECIMAL( 17, 2 ) ), 0 ) AS IndexPercent,
CASE
    WHEN LEN ( RecordCount ) > 9 THEN CAST( CAST( LEFT( RecordCount, LEN ( RecordCount ) - 9 ) AS VARCHAR(30) ) + ' Billion Records' AS VARCHAR)
    WHEN LEN ( RecordCount ) > 6 THEN CAST( CAST( LEFT( RecordCount, LEN ( RecordCount ) - 6 ) AS VARCHAR(30) ) + ' Million Records' AS VARCHAR)
    WHEN LEN ( RecordCount ) > 3 THEN CAST( CAST( LEFT( RecordCount, LEN ( RecordCount ) - 3 ) AS VARCHAR(30) ) + ' Thousand Records' AS VARCHAR)
    ELSE CAST(CAST(RecordCount AS VARCHAR(30) ) + ' Records' AS VARCHAR(30) )
END AS RecordCount,
CAST( CAST( TotalSpaceKB AS DECIMAL(17, 2) ) 
    / CAST( NULLIF( RecordCount, 0 ) AS DECIMAL( 17, 2 ) ) AS DECIMAL( 17, 2 ) ) AS RecordSizeKB
FROM CTE2
ORDER BY TableSpaceGB DESC;