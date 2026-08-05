-- taskq MySQL 建库脚本（生产受控建库用）。
--
-- 与 AutoMigrate 的关系：两者**并存**，是同一份 schema 的两条建表路径。
--   - 测试/示例走 gormstore.Config{AutoMigrate: true}，由 GORM 依模型自动建表；
--   - 生产环境建议关掉 AutoMigrate，改由 DBA 执行本脚本，把 schema 变更纳入受控发布。
--
-- ⚠️ 本文件的列类型/长度/索引名**不是肉眼照抄 gorm tag 得来的**，而是对真实 MySQL 8
-- 跑一遍 AutoMigrate 后 `SHOW CREATE TABLE` 导出的真值。照抄 tag 会错，例如：
--   - 无 size tag 的 string 落 LONGTEXT、无 size tag 的 []byte 落 LONGBLOB，都不是 VARCHAR(191)；
--   - Go 的 int 在 MySQL 方言下落 BIGINT 而非 INT；
--   - 索引名由 GORM 依 index tag 生成，简写会让 AutoMigrate 在既有库上再补一份同义不同名的重复索引。
-- 修改模型后请重新导出核对，schema_guard_test.go 会做表级+列级双向比对拦截漂移。
--
-- 三张表名可配（gormstore.Config 的 LockTable / MemberTable / MembershipTable）：
-- 本脚本用默认表名 taskq_partition_lock / taskq_partition_lock_members / taskq_membership_members。
-- 若在 Config 里改了名，必须相应改这三段 DDL 的表名，否则应用建不上表。

-- taskq_queue_info：每个 (queue, partition) 的游标元数据。
-- range_id 是 CAS 用的租约代号（AllocateTaskIDRange 每次 +1，旧 owner 的写入随即失效），
-- ack_level 是已确认位点，next_task_id 是下一个待分配的任务号。
CREATE TABLE `taskq_queue_info` (
  `queue_name`    VARCHAR(191) NOT NULL,
  `partition_id`  BIGINT       NOT NULL,
  `range_id`      BIGINT       NOT NULL DEFAULT 1,
  `ack_level`     BIGINT       NOT NULL DEFAULT 0,
  `next_task_id`  BIGINT       NOT NULL DEFAULT 1,
  PRIMARY KEY (`queue_name`, `partition_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- taskq_tasks：任务本体。dispatched_at 为 NULL 表示待投递，非 NULL 表示已投递
-- （超过 visibility timeout 后由 ResetTimedOutTasks 置回 NULL 重投）。
-- 两条复合索引都以 (queue_name, partition_id, dispatched_at) 打头，服务
-- "取某分区待投递任务"的主查询路径；索引名与 GORM 依 index tag 自动生成的名字一致，
-- 避免 AutoMigrate 在按本脚本建好的库上再补一份同义但不同名的索引。
CREATE TABLE `taskq_tasks` (
  `queue_name`        VARCHAR(191) NOT NULL,
  `partition_id`      BIGINT       NOT NULL,
  `task_id`           BIGINT       NOT NULL,
  `routing_key`       VARCHAR(191) NOT NULL DEFAULT '',
  `routing_deadline`  DATETIME(3)  DEFAULT NULL,
  -- data 在 taskRow 上无 size tag，GORM 映射为 LONGBLOB（不是 BLOB）。
  `data`              LONGBLOB     NOT NULL,
  `attempt`           BIGINT       NOT NULL DEFAULT 0,
  `created_at`        DATETIME(3)  NOT NULL,
  `dispatched_at`     DATETIME(3)  DEFAULT NULL,
  PRIMARY KEY (`queue_name`, `partition_id`, `task_id`),
  KEY `idx_tasks_visibility` (`queue_name`, `partition_id`, `dispatched_at`, `routing_key`, `task_id`),
  KEY `idx_tasks_routing_deadline` (`queue_name`, `partition_id`, `dispatched_at`, `routing_deadline`, `task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- taskq_partition_config：queue 的读写分区数。
-- scope = '_default_' 是全局默认行，其余每行是某个 queue 的显式 override。
CREATE TABLE `taskq_partition_config` (
  `scope`       VARCHAR(191) NOT NULL,
  `num_read`    BIGINT       NOT NULL,
  `num_write`   BIGINT       NOT NULL,
  `updated_at`  DATETIME(3)  NOT NULL,
  PRIMARY KEY (`scope`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- taskq_partition_lock：dbleader 选主锁（每个 lock_name 一行 leader）。
-- version 是 Fencing Token，每次易主 +1；expire_at_ms 为绝对过期时刻（毫秒）。
-- 表名可配：gormstore.Config.LockTable，改名后需同步改这里。
CREATE TABLE `taskq_partition_lock` (
  `lock_name`     VARCHAR(191)    NOT NULL COMMENT '锁名称/组件名',
  `leader_ip`     VARCHAR(191)    NOT NULL COMMENT '当前持有锁的节点 IP:PORT',
  `expire_at_ms`  BIGINT          NOT NULL COMMENT '锁的绝对过期时间（Unix 毫秒）',
  `version`       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '每次易主 +1，Fencing Token',
  PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- taskq_partition_lock_members：选主锁的成员心跳表，leader 据 last_seen 判活并剔除过期成员。
-- 表名可配：gormstore.Config.MemberTable，改名后需同步改这里。
CREATE TABLE `taskq_partition_lock_members` (
  `lock_name`        VARCHAR(191) NOT NULL COMMENT '锁名称，与主锁表对应',
  `instance_id`      VARCHAR(191) NOT NULL COMMENT '进程实例唯一 ID，每次启动重新生成',
  `node_ip`          VARCHAR(191) NOT NULL COMMENT '节点 IP:PORT（展示用）',
  `mac_address`      VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '第一个非回环网卡 MAC',
  `hostname`         VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'os.Hostname()',
  `pid`              BIGINT       NOT NULL DEFAULT 0 COMMENT '进程号',
  `os`               VARCHAR(32)  NOT NULL DEFAULT '' COMMENT 'runtime.GOOS',
  `go_version`       VARCHAR(64)  NOT NULL DEFAULT '' COMMENT 'runtime.Version()',
  `service_version`  VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '业务服务版本，用户传入',
  `description`      VARCHAR(256) NOT NULL DEFAULT '' COMMENT '用户传入备注',
  `start_time`       DATETIME(3)  NOT NULL COMMENT '进程启动时间',
  `last_seen`        DATETIME(3)  NOT NULL COMMENT '最近一次心跳时间，leader 据此判活',
  -- 复合主键字节数 = (191 + 191) × 4 = 1528 ≤ InnoDB 索引键 3072 字节上限。
  PRIMARY KEY (`lock_name`, `instance_id`),
  KEY `idx_members_lock_lastseen` (`lock_name`, `last_seen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- taskq_membership_members：服务发现的成员注册表（matching 等服务的实例心跳）。
-- expires_at 到期即视为下线，PruneExpiredMembers 负责清理；draining 表示实例正在优雅下线。
-- 表名可配：gormstore.Config.MembershipTable，改名后需同步改这里。
CREATE TABLE `taskq_membership_members` (
  `service_name`   VARCHAR(191)     NOT NULL COMMENT '服务名，如 matching',
  -- rpc_address 存 IP 文本，size:45 足够容纳 IPv6 全展开形式。
  `rpc_address`    VARCHAR(45)      NOT NULL COMMENT '实例 RPC IP',
  -- rpc_port 对应 Go 的 uint16，GORM 映射为 SMALLINT UNSIGNED。
  `rpc_port`       SMALLINT UNSIGNED NOT NULL COMMENT '实例 RPC 端口',
  -- host_id 是带 size:64 的 []byte，落 VARBINARY(64)；不写 size 才会落 LONGBLOB。
  `host_id`        VARBINARY(64)    NOT NULL COMMENT '主机唯一标识',
  `session_start`  DATETIME(3)      NOT NULL COMMENT '本次会话启动时间',
  `heartbeat_at`   DATETIME(3)      NOT NULL COMMENT '最近一次心跳时间',
  `expires_at`     DATETIME(3)      NOT NULL COMMENT '记录过期时刻，到期视为下线',
  `draining`       TINYINT(1)       NOT NULL DEFAULT 0 COMMENT '是否正在优雅下线',
  `start_at`       BIGINT           NOT NULL DEFAULT 0 COMMENT '分片区间起点',
  `stop_at`        BIGINT           NOT NULL DEFAULT 0 COMMENT '分片区间终点',
  -- 复合主键字节数 = 191×4 + 45×4 + 2 = 764 + 180 + 2 = 946 ≤ 3072。
  PRIMARY KEY (`service_name`, `rpc_address`, `rpc_port`),
  -- idx_membership_heartbeat 覆盖 GetActiveMembers 的
  --   WHERE heartbeat_at >= ? AND expires_at > ?
  -- 该查询不带 service_name,故 idx_membership_active 的前缀列用不上。
  KEY `idx_membership_heartbeat` (`heartbeat_at`, `expires_at`),
  KEY `idx_membership_active` (`service_name`, `heartbeat_at`, `expires_at`),
  KEY `idx_membership_expires` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
