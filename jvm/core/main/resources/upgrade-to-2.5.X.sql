alter  table  session_processor  add    `processor_option`  VARCHAR(512);



CREATE TABLE IF NOT EXISTS `processor_resource`
(
    `id`             SERIAL PRIMARY KEY,
    `processor_id`   BIGINT   NOT NULL,
    `session_id`     VARCHAR(767),
    `server_node_id` INT      NOT NULL,
    `resource_type`  VARCHAR(255),
    `allocated`      BIGINT   NOT NULL default 0,
    `extention`      VARCHAR(512),
    `status`         VARCHAR(255),
    `pid`            INT      NOT NULL DEFAULT -1,
    `created_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1
  COLLATE latin1_swedish_ci;
CREATE  INDEX `idx_processor_id_processor_resource` ON `processor_resource` (`processor_id`);
CREATE  INDEX `idx_node_id_processor_resource` ON `processor_resource` (`server_node_id`);
CREATE  INDEX `idx_session_id_processor_resource` ON `processor_resource` (`session_id`);
CREATE  INDEX `idx_node_status_processor_resource` ON `processor_resource` (`server_node_id`,`resource_type`,`status`);



CREATE TABLE IF NOT EXISTS `node_resource`
(
    `resource_id`    SERIAL PRIMARY KEY,
    `server_node_id` BIGINT   NOT NULL,
    `resource_type`  VARCHAR(255),
    `total`          BIGINT   NOT NULL default 0,
    `used`           BIGINT   NOT NULL default 0,
    `pre_allocated`  BIGINT   NOT NULL default 0,
    `allocated`      BIGINT   NOT NULL DEFAULT 0,
    `extention`      VARCHAR(512),
    `status`         VARCHAR(255),
    `created_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARACTER SET latin1
  COLLATE latin1_swedish_ci;
CREATE  INDEX `idx_node_id_node_resource` ON `node_resource` (`server_node_id`);
CREATE  INDEX `idx_node_status_node_resource` ON `node_resource` (`server_node_id`,`status`);
CREATE UNIQUE INDEX `idx_u_node_resource` ON `node_resource` (`server_node_id`, `resource_type`);


CREATE TABLE IF NOT EXISTS `session_ranks`
(
    `container_id`   SERIAL PRIMARY KEY,
    `session_id`     VARCHAR(767),
    `server_node_id` INT          NOT NULL,
    `global_rank`    INT UNSIGNED NOT NULL,
    `local_rank`     INT UNSIGNED NOT NULL
) DEFAULT CHARACTER SET latin1
  COLLATE latin1_swedish_ci;

  CREATE  INDEX `idx_session_id_session_ranks` ON `session_ranks` (`session_id`);