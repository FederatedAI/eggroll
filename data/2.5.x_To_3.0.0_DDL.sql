ALTER TABLE `session_main`
	ADD COLUMN `status_reason` VARCHAR(255) NULL AFTER `status`,
	ADD COLUMN `before_status` VARCHAR(255) NULL AFTER `status_reason`;
ALTER TABLE `session_processor`
	ADD COLUMN `before_status` VARCHAR(255) NULL AFTER `status_reason`;