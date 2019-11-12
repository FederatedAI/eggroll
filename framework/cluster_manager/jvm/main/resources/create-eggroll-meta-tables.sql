-- insertSelective database if not exists
CREATE DATABASE IF NOT EXISTS `eggroll_meta`;

USE `eggroll_meta`;
-- insertSelective table for fdn_meta

-- table
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`store` (
  `store_id` SERIAL PRIMARY KEY,
  `store_type` VARCHAR(255) NOT NULL,
  `namespace` VARCHAR(2000) NOT NULL DEFAULT 'DEFAULT',
  `name` VARCHAR(2000) NOT NULL,
  `total_partitions` INT UNSIGNED NOT NULL,
  `partitioner` VARCHAR(2000) NOT NULL DEFAULT 'JAVA_HASH',
  `serdes` VARCHAR(2000) NOT NULL DEFAULT '',
  `version` INT UNSIGNED NOT NULL DEFAULT 0,
  `status` VARCHAR(255) NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE UNIQUE INDEX `idx_u_store_n_tn` ON `eggroll_meta`.`store` (`store_type`(68), `namespace`(100), `name`(600));
CREATE INDEX `idx_store_tt` ON `eggroll_meta`.`store` (`store_type`(255));
CREATE INDEX `idx_store_ns` ON `eggroll_meta`.`store` (`namespace`(768));
CREATE INDEX `idx_store_n` ON `eggroll_meta`.`store` (`name`(768));
CREATE INDEX `idx_store_st` ON `eggroll_meta`.`store` (`status`(255));
CREATE INDEX `idx_store_v` ON `eggroll_meta`.`store` (`version`);


-- fragment
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`partition` (
  `partition_id` SERIAL PRIMARY KEY,                        -- self-increment sequence
  `store_id` BIGINT UNSIGNED NOT NULL,
  `node_id` BIGINT UNSIGNED NOT NULL,
  `store_partition_id` VARCHAR(255) UNSIGNED NOT NULL,      -- partition id of a store
  `status` VARCHAR(255) NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE UNIQUE INDEX `idx_u_partition_s_sp_n` ON `eggroll_meta`.`partition` (`store_id`, `store_partition_id`, `node_id`);
CREATE INDEX `idx_partition_ti` ON `eggroll_meta`.`partition` (`table_id`);
CREATE INDEX `idx_partition_ni` ON `eggroll_meta`.`partition` (`node_id`);
CREATE INDEX `idx_partition_s` ON `eggroll_meta`.`partition` (`status`(255));


-- node
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`node` (
  `node_id` SERIAL PRIMARY KEY,
  `host` VARCHAR(1000),
  `ip` VARCHAR(255) NOT NULL,
  `port` INT NOT NULL,
  `type` VARCHAR(255) NOT NULL,
  `status` VARCHAR(255) NOT NULL,
  `last_heartbeat_at` DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE INDEX `idx_node_h` ON `eggroll_meta`.`node` (`host`(768));
CREATE INDEX `idx_node_i` ON `eggroll_meta`.`node` (`ip`(255));
CREATE INDEX `idx_node_t` ON `eggroll_meta`.`node` (`type`(255));
CREATE INDEX `idx_node_s` ON `eggroll_meta`.`node` (`status`(255));
