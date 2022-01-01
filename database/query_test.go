package database

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockDb struct {
	query          string
	args           []interface{}
	expectedResult sql.Result
	expectedRows   *sql.Rows
	expectedErr    error
}

func (m *mockDb) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.query = query
	m.args = args
	return m.expectedResult, m.expectedErr
}

func (m *mockDb) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.query = query
	m.args = args
	return m.expectedRows, m.expectedErr
}

type test1 struct {
	P0 int `json:"j0"`
	P1 int `json:"j1"   pk:"true" fk:"true"`
	P2 int `column:"c2" pk:"true"`
	P3 int `column:"c3" pk:"t"`
	P4 int `column:"c4" fk:"1"`
	p5 int `column:"c5"`
	P6 int `map:"s"`
}

type test2 struct {
	P0 int `column:"c0" pk:"bad"`
}

type test3 struct {
	P0 int `column:"c0" fk:"bad"`
}

type test4 struct {
	A int
	B int
}

type test5 struct {
	P1 int `column:"c1" pk:"true"`
	P2 int `column:"c1" pk:"t"`
}

type test6 struct {
	P1 int `column:"c1"`
	P2 int `column:"c2"`
}

type test7 struct {
	P1 int `column:"c1" pk:"generated"`
	P2 int `column:"c2"`
}

type test8 struct {
	P1 int `column:"c1" pk:"generated"`
	P2 int `column:"c2" pk:"generated"`
}

type test9 struct {
	P1 string `column:"c1" pk:"generated"`
}

var (
	expectedColumns1 = map[string]*columnDef{
		`"p1"`: {fieldName: "P1", primaryKey: true, foreignKey: true},
		`"c2"`: {fieldName: "P2", primaryKey: true, foreignKey: false},
		`"c3"`: {fieldName: "P3", primaryKey: true, foreignKey: false},
		`"c4"`: {fieldName: "P4", primaryKey: false, foreignKey: true},
	}
	expectedColumns6 = map[string]*columnDef{
		`"c1"`: {fieldName: "P1", primaryKey: true, foreignKey: false},
		`"c2"`: {fieldName: "P2", primaryKey: false, foreignKey: false},
	}
	expectedColumns7 = map[string]*columnDef{
		`"c1"`: {fieldName: "P1", primaryKey: true, foreignKey: false},
		`"c2"`: {fieldName: "P2", primaryKey: false, foreignKey: false},
	}
	page1 = &TablePage{Start: 0, Size: 2}
	page2 = &TablePage{Start: 1, Size: 2}
)

func Test_NewTable(t *testing.T) {
	tests := map[string]struct {
		schema          string
		table           string
		structure       interface{}
		expectedErr     error
		expectedColumns map[string]*columnDef
		expectedGenCol  string
	}{
		"Undefined table":   {"schema", "", test1{}, ErrUndefinedTableName, nil, ""},
		"Invalid strict":    {"schema", "test1", 4, ErrNotAStructure, nil, ""},
		"Undefined struct":  {"schema", "test1", nil, ErrNotAStructure, nil, ""},
		"Pointer":           {"schema", "test1", &test1{}, nil, expectedColumns1, ""},
		"ByValue":           {"schema", "test1", test1{}, nil, expectedColumns1, ""},
		"Bad primary key":   {"schema", "test2", test2{}, ErrInvalidPrimaryKey, nil, ""},
		"Bad foreign key":   {"schema", "test3", test3{}, ErrInvalidForeignKey, nil, ""},
		"No columns":        {"schema", "test4", test4{}, ErrNoColumnsDefined, nil, ""},
		"Duplicate column":  {"schema", "test5", test5{}, ErrDuplicateColumn, nil, ""},
		"No primary key":    {"schema", "test6", test6{}, nil, expectedColumns6, ""},
		"Generated key":     {"schema", "test7", test7{}, nil, expectedColumns7, `"c1"`},
		"Multiple gen keys": {"schema", "test8", test8{}, ErrMultipleGeneratedPks, nil, ""},
		"Invalid key type":  {"schema", "test9", test9{}, ErrInvalidGenKey, nil, ""},
	}
	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			table, err := NewTable(tt.schema, tt.table, &tt.structure)
			if tt.expectedErr != nil {
				assert.EqualError(t, err, tt.expectedErr.Error())
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			} else {
				if tt.expectedColumns != nil {
					assert.Equal(t, tt.expectedColumns, table.columns)
				}

				if tt.expectedGenCol != "" {
					assert.Equal(t, tt.expectedGenCol, table.generatedKey)
				}
			}
		})
	}
}

func Test_Insert(t *testing.T) {
	table, err := NewTable("schema", "table", test1{})
	assert.Nil(t, err)
	t1 := test1{1, 2, 3, 4, 5, 6, 7}

	exec := mockDb{}
	_, _ = table.Insert(&exec, t1)

	assert.Equal(t, `INSERT INTO "schema"."table" ("p1","c2","c3","c4") VALUES ($1,$2,$3,$4)`, exec.query)
	assert.Equal(t, []interface{}{2, 3, 4, 5}, exec.args)
}

func Test_SelectOne(t *testing.T) {
	table, err := NewTable("schema", "table", test1{})
	assert.Nil(t, err)
	t1 := test1{1, 2, 3, 4, 5, 6, 7}

	exec := mockDb{}
	_, _ = table.SelectOne(&exec, t1)

	assert.Equal(t, `SELECT "p1","c2","c3","c4" FROM "schema"."table" WHERE "p1"=$1 AND "c2"=$2 AND "c3"=$3`, exec.query)
	assert.Equal(t, []interface{}{2, 3, 4}, exec.args)
}

func Test_ReadAll(t *testing.T) {
	tests := map[string]struct {
		structure       interface{}
		page            *TablePage
		expectedErr     error
		expectedQuery   string
		expectedColumns []interface{}
	}{
		"All rows": {
			structure:       test1{},
			page:            nil,
			expectedErr:     nil,
			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM "schema"."table"`,
			expectedColumns: nil,
		},
		"Page 1": {
			structure:       test1{},
			page:            page1,
			expectedErr:     nil,
			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM (SELECT "p1","c2","c3","c4", RANK() OVER (ORDER BY "c4","c2","c3") ranking FROM "schema"."table") x WHERE ranking >=%s AND ranking < %s`,
			expectedColumns: []interface{}{0, 2},
		},
		"Page 2": {
			structure:       test1{},
			page:            page2,
			expectedErr:     nil,
			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM (SELECT "p1","c2","c3","c4", RANK() OVER (ORDER BY "c4","c2","c3") ranking FROM "schema"."table") x WHERE ranking >=%s AND ranking < %s`,
			expectedColumns: []interface{}{2, 4},
		},
	}
	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			table, err := NewTable("schema", "table", tt.structure)
			assert.Nil(t, err)

			exec := mockDb{}
			if tt.page == nil {
				_, err = table.SelectAll(&exec)
			} else {
				_, err = table.SelectAllPaged(&exec, tt.page)
			}

			if tt.expectedErr != nil {
				assert.EqualError(t, err, tt.expectedErr.Error())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedQuery, exec.query)
				assert.Equal(t, tt.expectedColumns, exec.args)
			}
		})
	}
}

//func Test_ReadAllForeign(t *testing.T) {
//	tests := map[string]struct {
//		structure       interface{}
//		page            *TablePage
//		expectedErr     error
//		expectedQuery   string
//		expectedColumns []interface{}
//	}{
//		"All rows": {
//			structure:       &test1{},
//			page:            nil,
//			expectedErr:     nil,
//			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM "schema"."table" WHERE "c4"=$1 AND "p1"=$2`,
//			expectedColumns: []interface{}{5, 2},
//		},
//		"All rows. No foreign keys defined": {
//			structure:       &test6{},
//			page:            nil,
//			expectedErr:     ErrUndefinedFK,
//			expectedQuery:   "",
//			expectedColumns: nil,
//		},
//		"Page 1": {
//			structure:       &test1{},
//			page:            page1,
//			expectedErr:     nil,
//			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM (SELECT "p1","c2","c3","c4", RANK() OVER (ORDER BY "c4","c2","c3") ranking FROM "schema"."table" WHERE "c4"=$1 AND "p1"=$2) x WHERE ranking >=%s AND ranking < %s`,
//			expectedColumns: []interface{}{5, 2, 0, 2},
//		},
//		"Page 2": {
//			structure:       &test1{},
//			page:            page2,
//			expectedErr:     nil,
//			expectedQuery:   `SELECT "p1","c2","c3","c4" FROM (SELECT "p1","c2","c3","c4", RANK() OVER (ORDER BY "c4","c2","c3") ranking FROM "schema"."table" WHERE "c4"=$1 AND "p1"=$2) x WHERE ranking >=%s AND ranking < %s`,
//			expectedColumns: []interface{}{5, 2, 2, 4},
//		},
//		"Paged rows. No foreign keys defined": {
//			structure:       &test6{},
//			page:            page1,
//			expectedErr:     ErrUndefinedFK,
//			expectedQuery:   "",
//			expectedColumns: nil,
//		},
//	}
//	for testName, tt := range tests {
//		t.Run(testName, func(t *testing.T) {
//			table, err := NewTable("schema", "table", tt.structure)
//			assert.Nil(t, err)
//			t1 := test1{1, 2, 3, 4, 5, 6, 7}
//
//			exec := mockDb{}
//			if tt.page == nil {
//				_, err = table.SelectByForeignKey(&exec, t1)
//			} else {
//				_, err = table.SelectByForeignKeyPaged(&exec, t1, tt.page)
//			}
//
//			if tt.expectedErr != nil {
//				assert.EqualError(t, err, tt.expectedErr.Error())
//			} else {
//				assert.Nil(t, err)
//				assert.Equal(t, tt.expectedQuery, exec.query)
//				assert.Equal(t, tt.expectedColumns, exec.args)
//			}
//		})
//	}
//}

func Test_Update(t *testing.T) {
	table, err := NewTable("schema", "table", test1{})
	assert.Nil(t, err)
	t1 := test1{1, 2, 3, 4, 5, 6, 7}

	exec := mockDb{}
	_, _ = table.Update(&exec, t1)

	assert.Equal(t, `UPDATE "schema"."table" SET ("p1","c2","c3","c4") = ($1,$2,$3,$4) WHERE "p1"=$1 AND "c2"=$2 AND "c3"=$3`, exec.query)
	assert.Equal(t, []interface{}{2, 3, 4}, exec.args)
}

func Test_Delete(t *testing.T) {
	table, err := NewTable("schema", "table", test1{})
	assert.Nil(t, err)
	t1 := test1{1, 2, 3, 4, 5, 6, 7}

	exec := mockDb{}
	_, _ = table.Delete(&exec, t1)

	assert.Equal(t, `DELETE FROM "schema"."table" WHERE "p1"=$1 AND "c2"=$2 AND "c3"=$3`, exec.query)
	assert.Equal(t, []interface{}{2, 3, 4}, exec.args)
}

func Test_getColumnValues(t *testing.T) {
	table, _ := NewTable("schema", "table", test1{})
	t1 := test1{1, 2, 3, 4, 5, 6, 7}
	t2 := struct {
		P2 int
		P3 int
	}{2, 3}
	tests := map[string]struct {
		source         interface{}
		columns        []string
		expectedValues []interface{}
		expectedNames  []string
		expectedErr    error
	}{
		"ByValue": {
			source:         t1,
			columns:        []string{`"p1"`, `"c2"`, `"c3"`, `"c4"`},
			expectedValues: []interface{}{2, 3, 4, 5},
			expectedNames:  []string{`"p1"`, `"c2"`, `"c3"`, `"c4"`},
		},
		"Pointer": {
			source:         &t1,
			columns:        []string{`"p1"`, `"c2"`, `"c3"`, `"c4"`},
			expectedValues: []interface{}{2, 3, 4, 5},
			expectedNames:  []string{`"p1"`, `"c2"`, `"c3"`, `"c4"`},
		},
		"unknown column": { // Not possible, but will test anyway
			source:         &t1,
			columns:        []string{`"p1"`, `"c2"`, `"c3"`, `"c5"`},
			expectedValues: []interface{}{2, 3, 4},
			expectedNames:  []string{`"p1"`, `"c2"`, `"c3"`},
		},
		"alternative structure": {
			source:         t2,
			columns:        []string{`"p1"`, `"c2"`, `"c3"`, `"c5"`},
			expectedValues: []interface{}{2, 3},
			expectedNames:  []string{`"c2"`, `"c3"`},
		},
		"not a structure": {
			source:      "Not a structure",
			columns:     []string{`"p1"`, `"c2"`, `"c3"`, `"c5"`},
			expectedErr: ErrNotAStructure,
		},
	}
	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			names, values, err := table.columnValues(tt.source, tt.columns)
			if tt.expectedErr != nil {
				assert.Equal(t, tt.expectedErr, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedValues, values)
				assert.Equal(t, tt.expectedNames, names)
			}
		})
	}
}

func Test_Live(t *testing.T) {
	InitDB(t)
	defer Disconnect(t)

	var err error
	var id int64
	var rows *sql.Rows
	var count int64
	var result sql.Result
	var parent, child *TableDef

	// create a reference to the parent table
	if parent, err = NewTable(DatabaseSchema, "parent", &Parent{}); err != nil {
		t.Errorf("failed to create reference to parent table: %v", err)
		t.FailNow()
	}
	if rows, err = parent.SelectAll(db); err != nil {
		t.Errorf("Unable to retrieve rows from parent: %v", err)
		t.FailNow()
	}
	assert.False(t, rows.Next())

	// create a reference to the child table
	if child, err = NewTable(DatabaseSchema, "child", &Child{}); err != nil {
		t.Errorf("failed to create reference to child table: %v", err)
		t.FailNow()
	}
	if rows, err = child.SelectAll(db); err != nil {
		t.Errorf("Unable to retrieve rows from child: %v", err)
		t.FailNow()
	}
	assert.False(t, rows.Next())

	// Insert a single parent record
	p := Parent{
		ParentId: 1,
		Float:    1.01,
		Data:     "parent1",
		Dttm:     time.Unix(123456789, 0),
	}
	result, err = parent.Insert(db, &p)
	if err != nil {
		t.Errorf("Failed to write parent row: %v", err)
		t.FailNow()
	}
	if count, err = result.RowsAffected(); err != nil {
		t.Errorf("Failed to retrieve affected count from parent insert: %v", err)
		t.FailNow()
	}
	assert.Equal(t, int64(1), count)

	// Insert 10 child records
	c := ChildNoKey{
		ParentId: 1,
		Data:     "child1",
		Dttm:     time.Unix(234567890, 0),
	}
	for i := 1; i <= 10; i++ {
		result, err = child.Insert(db, c)
		if err != nil {
			t.Errorf("Failed to write child row: %v", err)
			t.FailNow()
		}
		if count, err = result.RowsAffected(); err != nil {
			t.Errorf("Failed to retrieve affected count from child insert: %v", err)
			t.FailNow()
		}
		assert.Equal(t, int64(1), count)
		if id, err = result.LastInsertId(); err != nil {
			if !strings.EqualFold(err.Error(), "LastInsertId is not supported by this driver") {
				t.Errorf("Failed to retrieve last insert id from child insert: %v", err)
				t.FailNow()
			}
		} else {
			assert.Equal(t, i, id)
		}
	}

	if rows, err = parent.SelectOne(db, p); err != nil {
		t.Errorf("Failed to retrieve one parent record: %v", err)
		t.FailNow()
	}
	assert.NotNil(t, rows)
	assert.True(t, rows.Next())
	i, err := parent.ExtractRow(rows)
	if err != nil {
		t.Errorf("Failed to extract parent row")
		t.FailNow()
	}
	pp := i.(*Parent)
	assert.Equal(t, p.ParentId, pp.ParentId)
	assert.Equal(t, p.Float, pp.Float)
	assert.Equal(t, p.Data, pp.Data)
	assert.Equal(t, p.Dttm.In(time.UTC), pp.Dttm.In(time.UTC))

}
