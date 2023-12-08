ALTER TABLE `session_main`
	ADD COLUMN `status_reason` VARCHAR(255) NULL AFTER `status`;
	ADD COLUMN `before_status` VARCHAR(255) NULL AFTER `status_reason`;

ALTER TABLE `session_processor`
	ADD COLUMN `before_status` VARCHAR(255) NULL AFTER `status_reason`;

ALTER TABLE `store_locator`
    ADD COLUMN `key_serdes_type` INT NOT NULL DEFAULT 0;
    ADD COLUMN `value_serdes_type` INT NOT NULL DEFAULT 0;
    ADD COLUMN `partitioner_type` INT NOT NULL DEFAULT 0;
    ADD COLUMN `version` INT UNSIGNED  NOT NULL DEFAULT 0;