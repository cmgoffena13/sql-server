USE [msdb];
GO

/*====================================================================================================
Active Jobs - Cortland Goffena

Shows current running jobs, status, their runtime, and the last step that could execute
====================================================================================================*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SELECT
j.name AS JobName,
a.start_execution_date AS StartDate,
'Step ' + CAST( js.step_id AS VARCHAR(3) ) + ': ' + js.step_name AS JobStatusDetails,
RIGHT( '0' + CAST( DATEDIFF( SECOND, a.start_execution_date, CAST( GETDATE() AS DATETIME ) ) / 3600 AS VARCHAR(2) ), 2 ) + ':' + 
    RIGHT( '0' + CAST( ( DATEDIFF( SECOND, a.start_execution_date, CAST( GETDATE() AS DATETIME ) ) / 60 ) % 60 AS VARCHAR(2) ), 2 ) + ':' + 
    RIGHT( '0' + CAST( DATEDIFF( SECOND, a.start_execution_date, CAST( GETDATE() AS DATETIME) ) % 60 AS VARCHAR(2) ), 2 ) AS RunTime,
mx.LastStep,
js.subsystem AS StepSubSystem,
c.name AS CategoryName
FROM dbo.sysjobs AS j
INNER JOIN msdb.dbo.syscategories AS c
	ON c.category_id = j.category_id
INNER JOIN dbo.sysjobactivity AS a
    ON a.job_id = j.job_id
INNER JOIN dbo.sysjobsteps AS js
    ON js.job_id = a.job_id
    AND js.step_id = ISNULL( a.last_executed_step_id, 0 ) + 1
INNER JOIN dbo.syssessions AS s
    ON s.session_id = a.session_id
INNER JOIN ( SELECT MAX( agent_start_date ) AS max_agent_start_time FROM dbo.syssessions ) AS s_max
    ON s_max.max_agent_start_time = s.agent_start_date
CROSS APPLY(
    SELECT TOP 1
    'Step ' + CAST( step_id AS VARCHAR(3) ) AS LastStep
    FROM dbo.sysjobsteps AS sjs
    WHERE sjs.job_id = j.job_id
) AS mx
WHERE a.run_requested_date IS NOT NULL
    AND a.stop_execution_date IS NULL
ORDER BY a.start_execution_date ASC