USE {database_name};
GO


SELECT TOP 50
CONCAT( (
    SELECT DATEDIFF( DAY, sqlserver_start_time, GETDATE() ) AS DaysSinceLastRestart FROM sys.dm_os_sys_info
), ' Days Ago' ) AS LastRestart,
CONCAT( SCHEMA_NAME ( o.schema_id ), '.', OBJECT_NAME ( p.object_id, p.database_id ) ) AS ProcedureName,
qp.query_plan AS ExecutionPlan,
p.execution_count AS ExecutionCount,
CAST( 
    CAST( 
        ROUND( CAST( ( p.total_elapsed_time / p.execution_count ) / 1000000 AS DECIMAL ), 0 ) 
    AS INT ) 
AS VARCHAR(30) ) + ' Seconds' AS AvgElapsedTime,
CAST( 
    CAST( 
        ROUND( CAST( p.last_elapsed_time / 1000000 AS DECIMAL ), 0 ) 
    AS INT ) 
AS VARCHAR(30) ) + ' Seconds' AS LastElapsedTime,
CASE 
    WHEN LEN( p.total_logical_reads / p.execution_count ) > 9 THEN CAST( CAST( LEFT( p.total_logical_reads / p.execution_count, LEN( p.total_logical_reads / p.execution_count ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( p.total_logical_reads / p.execution_count ) > 6 THEN CAST( CAST( LEFT( p.total_logical_reads / p.execution_count, LEN( p.total_logical_reads / p.execution_count ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( p.total_logical_reads / p.execution_count ) > 3 THEN CAST( CAST( LEFT( p.total_logical_reads / p.execution_count, LEN( p.total_logical_reads / p.execution_count ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( p.total_logical_reads / p.execution_count AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS AvgLogicalReads,
CASE 
    WHEN LEN( p.last_logical_reads ) > 9 THEN CAST( CAST( LEFT( p.last_logical_reads, LEN( p.last_logical_reads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( p.last_logical_reads ) > 6 THEN CAST( CAST( LEFT( p.last_logical_reads, LEN( p.last_logical_reads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( p.last_logical_reads ) > 3 THEN CAST( CAST( LEFT( p.last_logical_reads, LEN( p.last_logical_reads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( last_logical_reads AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS LastLogicalReads,
CAST( 
    CAST( 
        ROUND( CAST( ( p.total_worker_time / p.execution_count ) / 1000000 AS DECIMAL ), 0 ) 
    AS INT )
AS VARCHAR(30) ) + ' Seconds' AS AvgCPUTime,
CAST( 
    CAST( 
        ROUND( CAST( ( p.last_worker_time) / 1000000 AS DECIMAL), 0 )
    AS INT ) 
AS VARCHAR(30) ) + ' Seconds' AS LastCPUTime,
CASE 
    WHEN LEN( p.total_physical_reads / p.execution_count ) > 9 THEN CAST( CAST( LEFT( p.total_physical_reads / p.execution_count, LEN( p.total_physical_reads / p.execution_count ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( p.total_physical_reads / p.execution_count ) > 6 THEN CAST( CAST( LEFT( p.total_physical_reads / p.execution_count, LEN( p.total_physical_reads / p.execution_count ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( p.total_physical_reads / p.execution_count ) > 3 THEN CAST( CAST( LEFT( p.total_physical_reads / p.execution_count, LEN( p.total_physical_reads / p.execution_count ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( p.total_physical_reads / p.execution_count AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS AvgPhysicalReads,
CASE 
    WHEN LEN( p.last_physical_reads ) > 9 THEN CAST( CAST( LEFT( p.last_physical_reads, LEN( p.last_physical_reads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( p.last_physical_reads ) > 6 THEN CAST( CAST( LEFT( p.last_physical_reads, LEN( p.last_physical_reads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( p.last_physical_reads ) > 3 THEN CAST( CAST( LEFT( p.last_physical_reads, LEN( p.last_physical_reads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( p.last_physical_reads AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS LastPhysicalReads,
CASE 
    WHEN LEN( p.total_spills / p.execution_count ) > 9 THEN CAST( CAST( LEFT( p.total_spills / p.execution_count, LEN( p.total_spills / p.execution_count ) - 9 ) AS VARCHAR(30) ) + ' Billion Spills' AS VARCHAR(30) )
	WHEN LEN( p.total_spills / p.execution_count ) > 6 THEN CAST( CAST( LEFT( p.total_spills / p.execution_count, LEN( p.total_spills / p.execution_count ) - 6 ) AS VARCHAR(30) ) + ' Million Spills' AS VARCHAR(30) )
	WHEN LEN( p.total_spills / p.execution_count ) > 3 THEN CAST( CAST( LEFT( p.total_spills / p.execution_count, LEN( p.total_spills / p.execution_count ) - 3 ) AS VARCHAR(30) ) + ' Thousand Spills' AS VARCHAR(30) ) 
    ELSE CAST( CAST( p.total_spills / p.execution_count AS VARCHAR(30) )  + ' Spills' AS VARCHAR(30) )
END AS AvgSpills,
CASE 
    WHEN LEN( p.last_spills ) > 9 THEN CAST( CAST( LEFT( p.last_spills, LEN( p.last_spills ) - 9 ) AS VARCHAR(30) ) + ' Billion Spills' AS VARCHAR(30) )
	WHEN LEN( p.last_spills ) > 6 THEN CAST( CAST( LEFT( p.last_spills, LEN( p.last_spills ) - 6 ) AS VARCHAR(30) ) + ' Million Spills' AS VARCHAR(30) )
	WHEN LEN( p.last_spills ) > 3 THEN CAST( CAST( LEFT( p.last_spills, LEN( p.last_spills ) - 3 ) AS VARCHAR(30) ) + ' Thousand Spills' AS VARCHAR(30) ) 
    ELSE CAST( CAST( p.last_spills AS VARCHAR(30) )  + ' Spills' AS VARCHAR(30) )
END AS LastSpills,
DATEDIFF(MINUTE, p.last_execution_time, GETDATE() ) AS MinutesSinceLastExecutionTime,
p.last_execution_time AS LastExecutionTime,
p.cached_time AS CachedTime,
(
    SELECT sqlserver_start_time FROM sys.dm_os_sys_info
) AS LastRestartTime
FROM sys.dm_exec_procedure_stats AS p
INNER JOIN sys.objects AS o
    ON o.object_id = p.object_id
OUTER APPLY sys.dm_exec_query_plan( p.plan_handle ) AS qp
ORDER BY p.total_logical_reads DESC