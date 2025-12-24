-- schema.sql
-- TaskFlow Database Schema
-- Tasks table for metadata
CREATE TABLE IF NOT EXISTS tasks (
	id VARCHAR(36) PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	priority INTEGER NOT NULL CHECK (priority >= 1 AND priority <= 5),
	status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	payload_s3_key VARCHAR(1024), -- Reference to S3 object for large payloads
	error_message TEXT,
	retry_count INTEGER DEFAULT 0,
	max_retries INTEGER DEFAULT 3
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks (priority, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at);

-- Task execution history
CREATE TABLE IF NOT EXISTS task_executions (
	id SERIAL PRIMARY KEY,
	task_id VARCHAR(36) REFERENCES tasks(id) ON DELETE CASCADE,
	started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	completed_at TIMESTAMP WITH TIME ZONE,
	status VARCHAR(20) NOT NULL,
	error_message TEXT,
	execution_time_ms INTEGER
);

-- System statistics
CREATE TABLE IF NOT EXISTS system_stats (
	id SERIAL PRIMARY KEY,
	timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	metric_name VARCHAR(100) NOT NULL,
	metric_value DECIMAL(15,4) NOT NULL,
	tags JSONB
);

-- Function to update updated_at automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
	NEW.updated_at = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for automatic updated_at
CREATE TRIGGER update_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();