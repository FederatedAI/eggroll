-- insertSelective database if not exists
CREATE DATABASE IF NOT EXISTS `eggroll_meta`;

USE `eggroll_meta`;
-- insertSelective table for fdn_meta

-- store_locator
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`store_locator` (
  `store_locator_id` SERIAL PRIMARY KEY,
  `store_type` VARCHAR(255) NOT NULL,
  `namespace` VARCHAR(2000) NOT NULL DEFAULT 'DEFAULT',
  `name` VARCHAR(2000) NOT NULL,
  `path` VARCHAR(2000) NOT NULL DEFAULT '',
  `total_partitions` INT UNSIGNED NOT NULL,
  `partitioner` VARCHAR(2000) NOT NULL DEFAULT 'JAVA_HASH',
  `serdes` VARCHAR(2000) NOT NULL DEFAULT '',
  `version` INT UNSIGNED NOT NULL DEFAULT 0,
  `status` VARCHAR(255) NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE UNIQUE INDEX `idx_u_store_locator_n_tn` ON `eggroll_meta`.`store_locator` (`namespace`(120), `name`(640));
CREATE INDEX `idx_store_locator_tt` ON `eggroll_meta`.`store_locator` (`store_type`(255));
CREATE INDEX `idx_store_locator_ns` ON `eggroll_meta`.`store_locator` (`namespace`(768));
CREATE INDEX `idx_store_locator_n` ON `eggroll_meta`.`store_locator` (`name`(768));
CREATE INDEX `idx_store_locator_st` ON `eggroll_meta`.`store_locator` (`status`(255));
CREATE INDEX `idx_store_locator_v` ON `eggroll_meta`.`store_locator` (`version`);


-- store_partition
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`store_partition` (
  `store_partition_id` SERIAL PRIMARY KEY,          -- self-increment sequence
  `store_locator_id` BIGINT UNSIGNED NOT NULL,
  `node_id` BIGINT UNSIGNED NOT NULL,
  `partition_id` INT UNSIGNED NOT NULL,             -- partition id of a store
  `status` VARCHAR(255) NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE UNIQUE INDEX `idx_u_store_partition_s_sp_n` ON `eggroll_meta`.`store_partition` (`store_locator_id`, `store_partition_id`, `node_id`);
CREATE INDEX `idx_store_partition_si` ON `eggroll_meta`.`store_partition` (`store_locator_id`);
CREATE INDEX `idx_store_partition_ni` ON `eggroll_meta`.`store_partition` (`node_id`);
CREATE INDEX `idx_store_partition_s` ON `eggroll_meta`.`store_partition` (`status`(255));


-- node
CREATE TABLE IF NOT EXISTS `eggroll_meta`.`server_node` (
  `server_node_id` SERIAL PRIMARY KEY,
  `name` VARCHAR(2000) NOT NULL DEFAULT '',
  `server_cluster_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `host` VARCHAR(1000) NOT NULL,
  `port` INT NOT NULL,
  `node_type` VARCHAR(255) NOT NULL,
  `status` VARCHAR(255) NOT NULL,
  `last_heartbeat_at` DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE INDEX `idx_server_node_h_p_t` ON `eggroll_meta`.`server_node` (`host`, `port`, `node_type`)
CREATE INDEX `idx_server_node_h` ON `eggroll_meta`.`server_node` (`host`(768));
CREATE INDEX `idx_server_node_c` ON `eggroll_meta`.`server_node` (`server_cluster_id`);
CREATE INDEX `idx_server_node_t` ON `eggroll_meta`.`server_node` (`node_type`(255));
CREATE INDEX `idx_server_node_s` ON `eggroll_meta`.`server_node` (`status`(255));
