-- The pipeline raises an alert when one of its jobs fails (agb.services.pipeline,
-- `pipeline_job_failed`), so a corrupt panel body or a database error reaches the operator
-- when it happens rather than when the job's expected event falls overdue. Raising an alert
-- is an insert into agb.alert, which the pipeline could only read until now.

GRANT INSERT ON agb.alert TO pipeline;
