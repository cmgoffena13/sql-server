USE {database_name};
GO


DECLARE @ProcedureName VARCHAR(120) = 'schema.name'

DROP TABLE IF EXISTS #base
DROP TABLE IF EXISTs #agg

SELECT 
@ProcedureName AS ProcedureName,
ROW_NUMBER() OVER ( ORDER BY q.statement_start_offset ) AS StatementOrder,
SUBSTRING(
    st.text,
    ( q.statement_start_offset / 2 ) + 1,
    ( ( ( CASE 
            WHEN q.statement_end_offset = -1 THEN LEN( st.text ) 
            ELSE q.statement_end_offset
            END ) - q.statement_start_offset ) / 2 ) + 1
) AS QueryText,
CAST( ROUND( CAST( ( q.total_elapsed_time / NULLIF( q.execution_count, 0 ) ) / 1000000 AS DECIMAL ), 0 ) AS INT ) AS avg_elapsed_time_seconds,
q.total_logical_reads / NULLIF( q.execution_count, 0 ) AS avg_logical_reads,
CAST( ROUND ( CAST( ( q.total_worker_time / NULLIF(q.execution_count, 0 ) ) / 1000000 AS DECIMAL), 0 ) AS INT ) AS avg_cpu_time_seconds,
q.total_physical_reads / NULLIF( q.execution_count, 0 ) AS avg_physical_reads,
q.total_spills / NULLIF( q.execution_count, 0 ) AS avg_spills,
COUNT ( * ) OVER ( ORDER BY ( SELECT NULL ) ) AS TotalQueries,
pp.query_plan AS ProcedureExecutionPlan,
st.text AS ProcedureText,
q.execution_count AS ExecutionCount
INTO #base
FROM sys.dm_exec_procedure_stats AS p
INNER JOIN sys.objects AS o
    ON o.object_id = p.object_id
INNER JOIN sys.dm_exec_query_stats AS q
    ON q.sql_handle = p.sql_handle
CROSS APPLY sys.dm_exec_sql_text ( q.sql_handle ) st
CROSS APPLY sys.dm_exec_text_query_plan ( q.plan_handle, q.statement_start_offset, q.statement_end_offset ) qp
CROSS APPLY sys.dm_exec_query_plan ( p.plan_handle ) AS pp
WHERE p.object_id = OBJECT_ID ( @ProcedureName );


SELECT 
ProcedureName,
StatementOrder,
QueryText,
avg_elapsed_time_seconds,
avg_logical_reads,
avg_cpu_time_seconds,
avg_physical_reads,
avg_spills,
TotalQueries,
ProcedureExecutionPlan,
ProcedureText,
ExecutionCount,
SUM( avg_elapsed_time_seconds ) OVER ( ORDER BY ( SELECT NULL ) ) AS total_elapsed_time_seconds,
SUM( avg_logical_reads ) OVER ( ORDER BY ( SELECT NULL ) ) AS total_logical_reads,
SUM( avg_cpu_time_seconds ) OVER ( ORDER BY ( SELECT NULL ) ) AS total_cpu_time_seconds,
SUM( avg_physical_reads ) OVER ( ORDER BY ( SELECT NULL ) ) AS total_physical_reads,
SUM( avg_spills ) OVER ( ORDER BY ( SELECT NULL ) ) AS total_spills
INTO #agg
FROM #base


SELECT
ProcedureName,
StatementOrder,
QueryText,
CASE
    WHEN PATINDEX( '%#%', QueryText ) > 0 THEN 
        SUBSTRING(
            SUBSTRING( QueryText, PATINDEX( '%#%', QueryText ), ( LEN( QueryText ) - PATINDEX( '%#%', QueryText ) ) + 1 ),
            0,
            PATINDEX(
                '% %',
                SUBSTRING( QueryText, PATINDEX( '%#%', QueryText), ( LEN( QueryText ) - PATINDEX( '%#%', QueryText) ) + 1 )
            ) + 1
        )
    ELSE 'None'
END AS TempTable,
CAST( CAST( avg_elapsed_time_seconds AS DECIMAL ) / CAST( NULLIF( total_elapsed_time_seconds, 0 ) AS DECIMAL ) AS DECIMAL( 17, 2 ) ) AS ElapsedTimeDistribution,
CAST( CAST( avg_logical_reads AS DECIMAL ) / CAST( NULLIF( total_logical_reads, 0 ) AS DECIMAL ) AS DECIMAL( 17, 2 ) )  AS LogicalReadsDistribution,
CAST( CAST( avg_cpu_time_seconds AS DECIMAL ) / CAST( NULLIF( total_cpu_time_seconds, 0 ) AS DECIMAL ) AS DECIMAL( 17, 2 ) ) AS CPUTimeDistribution,
CAST( CAST( avg_physical_reads AS DECIMAL ) / CAST( NULLIF( total_physical_reads, 0 ) AS DECIMAL ) AS DECIMAL( 17, 2 ) ) AS PhysicalReadsDistribution,
CASE 
    WHEN total_spills > 0 THEN
        CAST( CAST( avg_spills AS DECIMAL ) / CAST( NULLIF( total_spills, 0 ) AS DECIMAL ) AS DECIMAL( 17, 2 ) )
    ELSE 0 
END AS SpillsDistribution,
CAST( avg_elapsed_time_seconds AS VARCHAR(30) ) + ' Seconds' AS AvgElapsedTimeSeconds,
CASE 
    WHEN LEN( avg_logical_reads ) > 9 THEN CAST( CAST( LEFT( avg_logical_reads, LEN( avg_logical_reads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
    WHEN LEN( avg_logical_reads ) > 6 THEN CAST( CAST( LEFT( avg_logical_reads, LEN( avg_logical_reads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
    WHEN LEN( avg_logical_reads ) > 3 THEN CAST( CAST( LEFT( avg_logical_reads, LEN( avg_logical_reads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( avg_logical_reads AS VARCHAR(30) ) + ' Reads' AS VARCHAR(30) ) 
END AS AvgLogicalReads,
CAST( avg_cpu_time_seconds AS VARCHAR(30) ) + ' Seconds' AS AvgCPUTimeSeconds,
CASE 
    WHEN LEN( avg_physical_reads) > 9 THEN CAST( CAST( LEFT( avg_physical_reads, LEN( avg_physical_reads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
    WHEN LEN( avg_physical_reads) > 6 THEN CAST( CAST( LEFT( avg_physical_reads, LEN( avg_physical_reads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
    WHEN LEN( avg_physical_reads) > 3 THEN CAST( CAST( LEFT( avg_physical_reads, LEN( avg_physical_reads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( avg_physical_reads AS VARCHAR(30) ) + ' Reads' AS VARCHAR(30) ) 
END AS AvgPhysicalReads,
CASE 
    WHEN LEN( avg_spills ) > 9 THEN CAST( CAST( LEFT( avg_spills, LEN( avg_spills ) - 9 ) AS VARCHAR(30) ) + ' Billion Spills' AS VARCHAR(30) )
    WHEN LEN( avg_spills ) > 6 THEN CAST( CAST( LEFT( avg_spills, LEN( avg_spills ) - 6 ) AS VARCHAR(30) ) + ' Million Spills' AS VARCHAR(30) )
    WHEN LEN( avg_spills ) > 3 THEN CAST( CAST( LEFT( avg_spills, LEN( avg_spills ) - 3 ) AS VARCHAR(30) ) + ' Thousand Spills' AS VARCHAR(30) ) 
    ELSE CAST( CAST( avg_spills AS VARCHAR(30) ) + ' Spills' AS VARCHAR(30) ) 
END AS AvgSpills,
ExecutionCount,
ProcedureExecutionPlan,
ProcedureText
FROM #agg 
ORDER BY StatementOrder