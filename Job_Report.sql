USE [msdb];
GO


DECLARE @JobName NVARCHAR(255) = 'JobName'


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


DROP TABLE IF EXISTS #History
SELECT TOP 100
sj.name AS JobName,
sj.enabled AS JobEnabled,
CONVERT (DATE, CONVERT (CHAR(8), sh.run_date), 112) AS StartDate,
'Not Running' AS JobStatus,
CASE 
    WHEN sh.run_status = 0 THEN  'Failed'
	WHEN sh.run_status = 1 THEN 'Succeeded'
	WHEN sh.run_status = 2 THEN 'Retry'
	WHEN sh.run_status = 3 THEN 'Canceled'
	WHEN sh.run_status = 4 THEN 'In Progress'
END AS JobStatusDetails,
1 AS JobFinished,
CAST( dbo.agent_datetime( sh.run_date, sh.run_time ) AS TIME(2) ) AS StartTime,
CAST( DATEADD( 
    SECOND, dbo.fn_Agent_DurationSeconds( sh.run_duration ), 
    dbo.agent_datetime( sh.run_date, sh.run_time ) 
    ) AS TIME(2) ) AS EndTime,
RIGHT( '0' + CAST( dbo.fn_Agent_DurationSeconds( sh.run_duration ) / 3600 AS NVARCHAR(2) ),2 ) + ':' + 
    RIGHT( '0' + CAST( ( dbo.fn_Agent_DurationSeconds( sh.run_duration ) / 60 ) % 60 AS VARCHAR(2) ),2 ) + ':' + 
    RIGHT( '0' + CAST( dbo.fn_Agent_DurationSeconds( sh.run_duration ) % 60 AS VARCHAR(2) ), 2 ) AS RunTime
INTO #JobHistory
FROM dbo.sysjobs AS sj
INNER JOIN dbo.sysjobhistory AS sh 
    ON sh.job_id = sj.job_id
WHERE sh.step_id=0
    AND sj.name = @JobName
ORDER BY dbo.agent_datetime(sh.run_date, sh.run_time) DESC

DROP TABLE IF EXISTS #JobActivity
SELECT
sj.name AS JobName,
sj.enabled AS JobEnabled,
CAST( sa.start_execution_date AS DATE ) AS StartDate,
'Running' AS JobStatus,
'Step ' + CAST( sjs.step_id AS VARCHAR(3) ) + ': ' + sjs.step_name AS JobStatusDetails,
CASE 
    WHEN sa.start_execution_date IS NOT NULL AND sa.stop_execution_date IS NOT NULL THEN 1 
    ELSE 0 
END AS JobFinished,
CAST( sa.start_execution_date AS TIME(2) ) AS StartTime,
CAST( '99:99:99.00' AS NVARCHAR(11) ) AS EndTime,
RIGHT( '0' + CAST( DATEDIFF( SECOND, sa.start_execution_date, CAST( GETDATE() AS DATETIME ) ) / 3600 AS NVARCHAR(2) ), 2 ) + ':' + 
    RIGHT( '0' + CAST( ( DATEDIFF( SECOND, sa.start_execution_date, CAST( GETDATE() AS DATETIME ) ) / 60 ) % 60 AS VARCHAR(2) ), 2 ) + ':' + 
    RIGHT( '0' + CAST( DATEDIFF( SECOND, sa.start_execution_date, CAST( GETDATE() AS DATETIME ) ) % 60 AS VARCHAR(2) ), 2 ) AS RunTime
INTO #JobActivity
FROM dbo.sysjobs AS sj
INNER JOIN dbo.sysjobactivity AS sa 
    ON SA.job_id = SJ.job_id
INNER JOIN dbo.sysjobsteps AS sjs 
    ON sjs.job_id = sa.job_id 
    AND sjs.step_id = ISNULL( sa.last_executed_step_id, 0 ) + 1
WHERE SA.session_id = ( SELECT MAX( session_id ) FROM msdb.dbo.sysjobactivity )
    AND SJ.name = @JobName
    AND SA.start_execution_date IS NOT NULL 
    AND SA.stop_execution_date IS NULL


SELECT
JobName,
JobEnabled,
StartDate,
JobStatus,
JobStatusDetails,
JobFinished,
CAST(StartTime AS NVARCHAR(11)) AS StartTime,
CAST(EndTime AS NVARCHAR(11)) AS EndTime,
RunTime
FROM #JobHistory
UNION ALL
SELECT
JobName,
JobEnabled,
StartDate,
JobStatus,
JobStatusDetails,
JobFinished,
CAST(StartTime AS NVARCHAR(11)) AS StartTime,
EndTime,
RunTime
FROM #JobActivity
ORDER BY StartDate DESC, StartTime DESC