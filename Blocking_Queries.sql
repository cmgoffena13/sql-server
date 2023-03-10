USE [master];
GO

/*====================================================================================================
Blocking Queries - Cortland Goffena

Shows current blocking queries and corresponding information about blockages
====================================================================================================*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SELECT
DB_NAME( tl.resource_database_id ) AS DatabaseName,
er.blocking_session_id AS BlockingSessionID,
tll.request_type AS BlockingRequestType,
ss.text AS BlockingSQLScript,
tl.request_session_id AS WaitingSessionID,
tl.request_type AS WaitingRequestType,
s.text AS WaitingSQLScript,
wt.resource_description AS ResourceDescription,
er.wait_type AS WaitType,
CASE
    WHEN wt.wait_duration_ms > 360000000 THEN 'Too Long'
    ELSE RIGHT('0' + CAST( ( wt.wait_duration_ms / 1000 ) / 3600 AS VARCHAR(2) ), 2 ) + ':' +
            RIGHT('0' + CAST( ( ( wt.wait_duration_ms / 1000 ) / 60 ) % 60 AS VARCHAR(2) ), 2 ) + ':' + 
            RIGHT('0' + CAST( ( wt.wait_duration_ms / 1000 ) % 60 AS VARCHAR(2) ), 2 )
END AS WaitTime,
tl.resource_associated_entity_id AS WaitingAssociatedEntity,
tl.resource_type AS WaitingResourceType
FROM sys.dm_tran_locks AS tl
INNER JOIN sys.dm_os_waiting_tasks AS wt
    ON wt.resource_address = tl.lock_owner_address
INNER JOIN sys.dm_exec_requests AS er
    ON er.session_id = tl.request_session_id
OUTER APPLY sys.dm_exec_sql_text ( er.sql_handle ) AS s
LEFT JOIN sys.dm_exec_requests AS b
    ON b.session_id = wt.blocking_session_id
OUTER APPLY sys.dm_exec_sql_text ( b.sql_handle ) AS ss
LEFT JOIN sys.dm_tran_locks AS tll
    ON tll.request_session_id = b.session_id