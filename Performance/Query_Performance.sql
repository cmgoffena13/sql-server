USE {database_name};
GO

/*====================================================================================================
Query Performance - Cortland Goffena

Shows queries with highest total logical reads to help pinpoint performance tuning opportunities
Note: stats may be reset by some sql server backend actions, as well as a restart
====================================================================================================*/

DECLARE @Top INT = 100


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DROP TABLE IF EXISTS #Queries
SELECT ( @Top + 100 ) /* Query plan can have multiple rows */
       query_plan_hash, 
       execution_count,
       total_elapsed_time,
       total_logical_reads,
       total_physical_reads,
       total_spills,
       creation_time,
       last_execution_time
INTO #Queries
FROM sys.dm_exec_query_stats
ORDER BY total_logical_reads DESC;


CREATE NONCLUSTERED INDEX IX_#Queries 
    ON #Queries ( query_plan_hash );


DROP TABLE IF EXISTS #Results
SELECT 
CONCAT ( 
    ( SELECT DATEDIFF ( DAY, sqlserver_start_time, GETDATE () ) AS DaysSinceLastRestart FROM sys.dm_os_sys_info ),
    ' Days Ago'
) AS LastRestart,
CASE
    WHEN PATINDEX( '%CREATE%PROCEDURE%', t.text ) = 0 THEN NULL
    WHEN PATINDEX( '%CREATE%FUNCTION', t.text ) > 0 THEN 'FUNCTION'
    WHEN PATINDEX( '%CREATE%PROCEDURE%', t.text ) > 0
        THEN REPLACE(REPLACE(
            SUBSTRING(
                SUBSTRING(
                    t.text,
                    PATINDEX( '%[[]%', t.text ),
                    ( LEN( t.text ) - PATINDEX( '%[[]%', t.text ) ) ),
                0,
                PATINDEX( '%][^.]%', 
                    SUBSTRING( t.text, 
                        PATINDEX( '%[[]%', t.text ), 
                        ( LEN( t.text ) - PATINDEX( '%[[]%', t.text ) ) )
                ) + 1
            ), '[', ''), ']', '')
    ELSE 'Unable to find Procedure Name'
END AS ProcedureName,
t.text AS Query,
pl.query_plan AS ExecutionPlan,
CAST( CAST( CAST( ( s.TotalElapsedTime / s.TotalExecutionCount ) / 1000000 AS DECIMAL ) AS INT ) AS VARCHAR(30) ) + ' Seconds' AS AvgElapsedTime,
CAST( CAST( CAST( ( s.TotalElapsedTime ) / 1000000 AS DECIMAL ) AS INT ) AS VARCHAR(30) ) + ' Seconds' AS TotalElapsedTime,
CASE 
    WHEN LEN( s.TotalLogicalReads / s.TotalExecutionCount ) > 9 THEN CAST( CAST( LEFT( s.TotalLogicalReads / s.TotalExecutionCount, LEN( s.TotalLogicalReads / s.TotalExecutionCount ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( s.TotalLogicalReads / s.TotalExecutionCount ) > 6 THEN CAST( CAST( LEFT( s.TotalLogicalReads / s.TotalExecutionCount, LEN( s.TotalLogicalReads / s.TotalExecutionCount ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( s.TotalLogicalReads / s.TotalExecutionCount ) > 3 THEN CAST( CAST( LEFT( s.TotalLogicalReads / s.TotalExecutionCount, LEN( s.TotalLogicalReads / s.TotalExecutionCount ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( s.TotalLogicalReads / s.TotalExecutionCount AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS AvgLogicalReads,
CASE
    WHEN LEN ( s.TotalLogicalReads ) > 9 THEN CAST( CAST( LEFT( s.TotalLogicalReads, LEN ( s.TotalLogicalReads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
    WHEN LEN ( s.TotalLogicalReads ) > 6 THEN CAST( CAST( LEFT( s.TotalLogicalReads, LEN ( s.TotalLogicalReads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
    WHEN LEN ( s.TotalLogicalReads ) > 3 THEN CAST( CAST( LEFT( s.TotalLogicalReads, LEN ( s.TotalLogicalReads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) )
    ELSE CAST( CAST( s.TotalLogicalReads AS VARCHAR(30) ) + ' Reads' AS VARCHAR(30) )
END AS TotalLogicalReads,
CASE 
    WHEN LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) > 9 THEN CAST( CAST( LEFT( s.TotalPhysicalReads / s.TotalExecutionCount, LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
	WHEN LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) > 6 THEN CAST( CAST( LEFT( s.TotalPhysicalReads / s.TotalExecutionCount, LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
	WHEN LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) > 3 THEN CAST( CAST( LEFT( s.TotalPhysicalReads / s.TotalExecutionCount, LEN( s.TotalPhysicalReads / s.TotalExecutionCount ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) ) 
    ELSE CAST( CAST( s.TotalPhysicalReads / s.TotalExecutionCount AS VARCHAR(30) )  + ' Reads' AS VARCHAR(30) )
END AS AvgPhysicalReads,
CASE
    WHEN LEN ( s.TotalPhysicalReads ) > 9 THEN CAST( CAST( LEFT( s.TotalPhysicalReads, LEN ( s.TotalPhysicalReads ) - 9 ) AS VARCHAR(30) ) + ' Billion Reads' AS VARCHAR(30) )
    WHEN LEN ( s.TotalPhysicalReads ) > 6 THEN CAST( CAST( LEFT( s.TotalPhysicalReads, LEN ( s.TotalPhysicalReads ) - 6 ) AS VARCHAR(30) ) + ' Million Reads' AS VARCHAR(30) )
    WHEN LEN ( s.TotalPhysicalReads ) > 3 THEN CAST( CAST( LEFT( s.TotalPhysicalReads, LEN ( s.TotalPhysicalReads ) - 3 ) AS VARCHAR(30) ) + ' Thousand Reads' AS VARCHAR(30) )
    ELSE CAST( CAST( s.TotalPhysicalReads AS VARCHAR(30) ) + ' Reads' AS VARCHAR(30) )
END AS TotalPhysicalReads,
CASE
    WHEN LEN ( s.TotalSpills ) > 9 THEN CAST( CAST( LEFT( s.TotalSpills, LEN ( s.TotalSpills ) - 9 ) AS VARCHAR(30) ) + ' Billion Spills' AS VARCHAR(30) )
    WHEN LEN ( s.TotalSpills ) > 6 THEN CAST( CAST( LEFT( s.TotalSpills, LEN ( s.TotalSpills ) - 6 ) AS VARCHAR(30) ) + ' Million Spills' AS VARCHAR(30) )
    WHEN LEN ( s.TotalSpills ) > 3 THEN CAST( CAST( LEFT( s.TotalSpills, LEN ( s.TotalSpills ) - 3 ) AS VARCHAR(30) ) + ' Thousand Spills' AS VARCHAR(30) )
    ELSE CAST( CAST( s.TotalSpills AS VARCHAR(30) ) + ' Spills' AS VARCHAR(30) )
END AS TotalSpills,
DATEDIFF ( MINUTE, s.LastExecutionTime, GETDATE () ) AS MinutesSinceLastExecutionTime,
s.LastExecutionTime,
s.CachedTime
INTO #Results
FROM
    (
        SELECT deqs.query_plan_hash,
               SUM ( deqs.execution_count ) AS TotalExecutionCount,
               SUM ( deqs.total_elapsed_time ) AS TotalElapsedTime,
               SUM ( deqs.total_logical_reads ) AS TotalLogicalReads,
               SUM ( deqs.total_physical_reads ) AS TotalPhysicalReads,
               SUM ( deqs.total_spills ) AS TotalSpills,
               MIN ( deqs.creation_time ) AS CachedTime,
               MAX ( deqs.last_execution_time ) AS LastExecutionTime
        FROM #Queries AS deqs
        GROUP BY deqs.query_plan_hash
    ) AS s
CROSS APPLY ( SELECT plan_handle 
              FROM sys.dm_exec_query_stats AS deqs 
              WHERE s.query_plan_hash = deqs.query_plan_hash ) AS p
CROSS APPLY sys.dm_exec_sql_text ( p.plan_handle ) AS t
CROSS APPLY sys.dm_exec_query_plan ( p.plan_handle ) AS pl
ORDER BY s.TotalLogicalReads DESC;


SELECT TOP ( @Top )
*
FROM #Results
/* Can then filter for any specific keywords */