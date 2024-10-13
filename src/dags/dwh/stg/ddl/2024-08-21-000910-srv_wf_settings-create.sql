CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR(2048) NOT NULL,
    workflow_settings JSON NOT NULL,

    CONSTRAINT srv_wf_settings_pkey PRIMARY KEY(id),
    CONSTRAINT srv_wf_settings_workflow_key_uindex UNIQUE(workflow_key)
);
