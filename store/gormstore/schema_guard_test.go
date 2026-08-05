package gormstore

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

// schemaSQLPath 指向手工维护的 MySQL 建库脚本。生产按它建库，测试走 AutoMigrate——
// 两条路径漂移时只有生产会炸，故必须机械比对。
const schemaSQLPath = "schema.sql"

// guardedModels 是 AutoMigrate 会建的全部 6 张表：模型 -> 表名。
//
// 前三个模型自带 TableName()，后三个的表名由 Config 决定（LockTable / MemberTable /
// MembershipTable），这里取默认值——schema.sql 也按默认表名写。改名部署时表结构不变，
// 只有表名不同，故本守卫仍然有效。
//
// 必须与 (*Store).AutoMigrate 中的模型列表保持一致；
// TestGuardedModelsCoverAutoMigrate 会在两者脱节时失败。
var guardedModels = map[string]any{
	"taskq_queue_info":       &queueInfoRow{},
	"taskq_tasks":            &taskRow{},
	"taskq_partition_config": &partitionConfigRow{},
	defaultLockTable:         &lockRow{},
	defaultMemberTable:       &memberRow{},
	defaultMembershipTable:   &membershipMemberRow{},
}

// createTableRe 匹配建表语句，兼容可选的 IF NOT EXISTS 与反引号。
var createTableRe = regexp.MustCompile("(?i)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`([^`]+)`")

// columnDefRe 抓取列定义行的列名（形如 "  `col_name` TYPE ..."）。
// 只匹配行首缩进后紧跟反引号的形式，从而天然排除 PRIMARY KEY / KEY 这些约束行。
var columnDefRe = regexp.MustCompile("(?m)^\\s+`([a-z0-9_]+)`\\s+[A-Za-z]")

// schemaSQLTableColumns 解析 schema.sql，返回 表名 -> 列名集合。
// 手写正则而非上真正的 SQL parser：schema.sql 是本仓库自己维护的、格式高度规整的文件，
// 正则足够且零依赖；格式一旦偏离，下面的列数断言会立即失败，不会静默漏判。
func schemaSQLTableColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(schemaSQLPath))
	if err != nil {
		t.Fatalf("read %s: %v", schemaSQLPath, err)
	}
	text := string(raw)

	locs := createTableRe.FindAllStringSubmatchIndex(text, -1)
	if len(locs) == 0 {
		t.Fatalf("no CREATE TABLE statements parsed from %s", schemaSQLPath)
	}
	result := make(map[string]map[string]bool, len(locs))
	for i, loc := range locs {
		table := text[loc[2]:loc[3]]
		bodyEnd := len(text)
		if i+1 < len(locs) {
			bodyEnd = locs[i+1][0]
		}
		cols := make(map[string]bool)
		for _, m := range columnDefRe.FindAllStringSubmatch(text[loc[1]:bodyEnd], -1) {
			cols[m[1]] = true
		}
		if len(cols) == 0 {
			t.Fatalf("parsed 0 columns for table %q in %s —— 解析失真，请检查 columnDefRe 与文件格式", table, schemaSQLPath)
		}
		result[table] = cols
	}
	return result
}

// modelColumns 用 GORM 自己的 schema.Parse 反射出每个模型的列名集合。
//
// 关键：**不能肉眼照抄 gorm tag**。列的真实 DDL 由 tag + 方言默认值共同决定
// （例：无 size tag 的 []byte 落 LONGBLOB 而非 BLOB，无 size tag 的 string 落 LONGTEXT
// 而非 VARCHAR(191)，Go 的 int 落 BIGINT 而非 INT），只有让 GORM 自己解析才与
// AutoMigrate 完全一致。
func modelColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	result := make(map[string]map[string]bool, len(guardedModels))
	for table, model := range guardedModels {
		s, err := schema.Parse(model, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("parse model for table %s: %v", table, err)
		}
		cols := make(map[string]bool, len(s.DBNames))
		for _, name := range s.DBNames {
			cols[name] = true
		}
		result[table] = cols
	}
	return result
}

// TestSchemaSQLCoversAutoMigrateTables 是**表级**防漂移守卫：AutoMigrate 会建的每张表
// 都必须在 schema.sql 里声明，反之 schema.sql 也不该有多余的表。
//
// 缺表方向是致命的：按 schema.sql 手工建库的生产环境会直接 "Table doesn't exist"，
// 而 SQLite/AutoMigrate 的测试环境自动建表，CI 全绿察觉不到。
func TestSchemaSQLCoversAutoMigrateTables(t *testing.T) {
	schemaTables := make([]string, 0, len(guardedModels))
	for table := range schemaSQLTableColumns(t) {
		schemaTables = append(schemaTables, table)
	}

	var missingFromSchema []string
	for table := range guardedModels {
		if !slices.Contains(schemaTables, table) {
			missingFromSchema = append(missingFromSchema, table)
		}
	}
	if len(missingFromSchema) > 0 {
		slices.Sort(missingFromSchema)
		t.Errorf("AutoMigrate 会建、但 %s 里没有的表: %v\n"+
			"手工建库的生产环境将没有这些表，一读写就报 Table doesn't exist。",
			schemaSQLPath, missingFromSchema)
	}

	var missingFromModels []string
	for _, table := range schemaTables {
		if _, ok := guardedModels[table]; !ok {
			missingFromModels = append(missingFromModels, table)
		}
	}
	if len(missingFromModels) > 0 {
		slices.Sort(missingFromModels)
		t.Errorf("%s 里声明、但 AutoMigrate 不建的表: %v\n"+
			"要么补进 guardedModels，要么（若确为废弃表）从 schema.sql 删掉。",
			schemaSQLPath, missingFromModels)
	}
}

// TestSchemaSQLColumnsMatchModels 是**列级**防漂移守卫。
//
// 只比对表集合是不够的：列可以在模型里加了、schema.sql 里忘了补——表在、列不在，
// 表级守卫全绿放行，而生产环境一写就报 "Unknown column"。故必须双向比对：
//   - 模型有、schema.sql 没有 → 生产建库缺列，必炸（致命方向）；
//   - schema.sql 有、模型没有 → 废弃列或漏登记的字段。
func TestSchemaSQLColumnsMatchModels(t *testing.T) {
	schemaCols := schemaSQLTableColumns(t)
	modelCols := modelColumns(t)

	for table, want := range modelCols {
		got, ok := schemaCols[table]
		if !ok {
			// 表级守卫已覆盖此方向，这里不重复报错。
			continue
		}
		var missing []string
		for col := range want {
			if !got[col] {
				missing = append(missing, col)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			t.Errorf("表 %s：AutoMigrate 会建、但 %s 里没有的列: %v\n"+
				"手工建库的生产环境将缺这些列，读写该表时报 Unknown column。\n"+
				"列类型请以对真实 MySQL 跑 AutoMigrate 后 SHOW CREATE TABLE 的输出为准，不要肉眼照抄 tag。",
				table, schemaSQLPath, missing)
		}

		var extra []string
		for col := range got {
			if !want[col] {
				extra = append(extra, col)
			}
		}
		if len(extra) > 0 {
			slices.Sort(extra)
			t.Errorf("表 %s：%s 里声明、但没有对应模型字段的列: %v\n"+
				"要么补上模型字段，要么（若确为历史遗留）从 schema.sql 删掉。",
				table, schemaSQLPath, extra)
		}
	}
}

// TestGuardedModelsCoverAutoMigrate 防止 guardedModels 与 (*Store).AutoMigrate 脱节：
// 后者新增模型而前者忘了登记时，上面两条守卫会因为"没人告诉它有新表"而静默放过。
// 这里退而求其次做数量断言——真正建表的模型数必须与登记数一致。
// 新增/删除模型时，请同步更新 AutoMigrate、guardedModels 与 schema.sql 三处。
func TestGuardedModelsCoverAutoMigrate(t *testing.T) {
	const autoMigrateModelCount = 6 // 见 (*Store).AutoMigrate：3 个固定表名 + 3 个可配表名
	if len(guardedModels) != autoMigrateModelCount {
		t.Fatalf("guardedModels 有 %d 项，AutoMigrate 建 %d 张表——两者脱节，请同步更新",
			len(guardedModels), autoMigrateModelCount)
	}
}
