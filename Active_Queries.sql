USE [master];
GO

/*====================================================================================================
Active Queries - Cortland Goffena

Shows current running queries and a multitude of details 
====================================================================================================*/

/*
KILL 194                        --Kills session
KILL 194 WITH STATUSONLY        --Shows progress on rollback after kill
*/

/* List of WaitTypes
https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql?view=sql-server-ver15

*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SELECT
er.session_id AS SessionID,
CASE WHEN er.blocking_session_id = 0 THEN NULL ELSE er.blocking_session_id END AS BlockedBy,
CAST( er.start_time AS DATETIME2(0) ) AS StartTime,
es.login_name AS LoginName,
DB_NAME( er.database_id ) AS DatabaseName,
CASE
    WHEN er.total_elapsed_time > 360000000 THEN 'Too Long'
    ELSE RIGHT( '0' + CAST( ( er.total_elapsed_time/1000 ) / 3600 AS VARCHAR(2)), 2 ) + ':' + 
         RIGHT( '0' + CAST( ( ( er.total_elapsed_time/1000 ) / 60) % 60 AS VARCHAR(2) ), 2 ) + ':' + 
         RIGHT( '0' + CAST( ( ( er.total_elapsed_time/1000 ) % 60 ) AS VARCHAR(2)), 2 )
END AS RunTime,
CASE
    WHEN er.wait_time > 360000000 THEN 'Too Long'
    ELSE RIGHT( '0' + CAST( ( er.wait_time/1000 ) / 3600 AS VARCHAR(2)), 2 ) + ':' + 
         RIGHT( '0' + CAST( ( ( er.wait_time/1000 ) / 60) % 60 AS VARCHAR(2) ), 2 ) + ':' + 
         RIGHT( '0' + CAST( ( ( er.wait_time/1000 ) % 60 ) AS VARCHAR(2)), 2 )
END AS CurrentWait,
CASE
    WHEN er.status = 'suspended' AND er.wait_type = 'RESOURCE_SEMAPHORE' THEN 'waiting: Out Of Available Memory'
    WHEN er.status = 'suspended' THEN 'waiting: ' + er.wait_type
    ELSE er.status
END AS [Status],
er.command AS Command,
CASE
    WHEN PATINDEX( '%CREATE%PROCEDURE%', s.text ) = 0 THEN NULL
    WHEN PATINDEX( '%CREATE%FUNCTION', s.text ) > 0 THEN 'FUNCTION'
    WHEN PATINDEX( '%CREATE%PROCEDURE%', s.text ) > 0
        THEN REPLACE(REPLACE(
            SUBSTRING(
                SUBSTRING(
                    s.text,
                    PATINDEX( '%[[]%', s.text ),
                    ( LEN( s.text ) - PATINDEX( '%[[]%', s.text ) )
                ),
                0,
                PATINDEX( '%][^.]%', 
                    SUBSTRING( s.text, 
                        PATINDEX( '%[[]%', s.text ), 
                        ( LEN( s.text ) - PATINDEX( '%[[]%', s.text ) ) )
                         ) + 1
            ), '[', ''), ']', '')
    ELSE 'Unable to find Procedure Name'
END AS ProcedureName,
SUBSTRING(
    s.text,
    ( er.statement_start_offset/2 ) + 1,
    ( ( CASE
            WHEN er.statement_end_offset = -1 THEN LEN( CONVERT( NVARCHAR(MAX), s.text ) ) * 2 
            ELSE er.statement_end_offset 
        END - er.statement_start_offset ) / 2 ) + 1
) AS SQLQuery,
s.text AS SQLScript,
qp.query_plan AS QueryPlan,
er.cpu_time / 60 AS CPUTimeMins,
er.logical_reads / 128 AS LogicalReadsMB,
er.reads / 128 AS PhysicalReadsMB,
er.writes / 128 AS PhysicalWritesMB,
CASE
    WHEN es.row_count IN ( 1, 0 ) THEN NULL
    ELSE es.row_count
END AS [RowCount],
CASE
    WHEN er.transaction_isolation_level = 0 THEN 'Unspecified'
    WHEN er.transaction_isolation_level = 1 THEN 'ReadUncommitted'
    WHEN er.transaction_isolation_level = 2 THEN 'ReadCommitted'
    WHEN er.transaction_isolation_level = 3 THEN 'Repeatable'
    WHEN er.transaction_isolation_level = 4 THEN 'Serializable'
    WHEN er.transaction_isolation_level = 5 THEN 'Snapshot'
    ELSE NULL
END AS TransactionIsolationLevel,
CASE
    WHEN er.percent_complete = 0 THEN NULL
    ELSE er.percent_complete
END AS PercentComplete,
CASE
    WHEN er.estimated_completion_time = 0 THEN NULL
    ELSE er.estimated_completion_time / ( 1000 * 60 )
END AS EstimatedCompletionTimeMins,
UPPER( es.host_name ) AS HostName,
UPPER( es.program_name ) AS ProgramName,
'>>>>>' AS [TraceFlag7412],
p.physical_operator_name AS OperatorName,
p.row_count AS CurrentRowCount,
p.scan_count AS ScanCount,
p.database_id AS DatabaseID,
p.object_id AS ObjectID,
p.index_id AS IndexID,
p.rewind_count AS RewindCount,
p.rebind_count AS RebindCount
FROM sys.dm_exec_requests AS er
INNER JOIN sys.dm_exec_sessions AS es
    ON es.session_id = er.session_id
OUTER APPLY sys.dm_exec_sql_text( er.sql_handle ) AS s
OUTER APPLY sys.dm_exec_query_plan( er.plan_handle ) AS qp
LEFT JOIN sys.dm_exec_query_profiles AS p
    ON p.session_id = er.session_id
WHERE er.session_id != @@SPID 
    AND es.is_user_process = 1 /* Removes most system processes */