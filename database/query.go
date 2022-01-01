package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
)

// Validation errors
var (
	ErrUndefinedTableName   = errors.New("table name not defined")
	ErrNotAStructure        = errors.New("not a structure")
	ErrInvalidPrimaryKey    = errors.New("invalid primary key marker")
	ErrInvalidForeignKey    = errors.New("invalid foreign key marker")
	ErrNoColumnsDefined     = errors.New("no columns defined")
	ErrDuplicateColumn      = errors.New("column can only be defined once")
	ErrUndefinedFK          = errors.New("undefined foreign key")
	ErrMultipleGeneratedPks = errors.New("only one generated primary key is permitted")
	ErrInvalidGenKey        = errors.New("generated key must be an integer type")
)

// Basic CRUD SQL templates (Postgres).
const (
	insertSQL           = "INSERT INTO %s (%%s) VALUES (%%s)"
	selectWhereSQL      = "SELECT %s FROM %s WHERE %s"
	selectWherePagedSQL = "SELECT %[1]s FROM (SELECT %[1]s, RANK() OVER (ORDER BY %[2]s) ranking FROM %[3]s WHERE %[4]s) x " +
		"WHERE ranking >=%%s AND ranking < %%s"
	selectAllSQL      = "SELECT %s FROM %s"
	selectAllPagedSQL = "SELECT %[1]s FROM (SELECT %[1]s, RANK() OVER (ORDER BY %[2]s) ranking FROM %[3]s) x " +
		"WHERE ranking >=%%s AND ranking < %%s"
	updateSQL = "UPDATE %s SET (%s) = (%s) WHERE %s"
	deleteSQL = "DELETE FROM %s WHERE %s"
)

// SQL Query names.
const (
	insertQuery        = "Insert"
	selectOneQuery     = "ReadOne"
	selectAllQuery     = "ReadAll"
	selectAllPageQuery = "ReadAllPage"
	//selectForeignQuery      = "ReadForeign"
	//selectForeignPagedQuery = "ReadForeignPaged"
	updateQuery = "Update"
	deleteQuery = "Delete"
)

type columnDef struct {
	fieldName  string
	primaryKey bool
	foreignKey bool
}

type TableDef struct {
	name         string
	table        interface{}
	tableType    reflect.Type
	columns      map[string]*columnDef
	generatedKey string
	fkColumns    []string
	pkColumns    []string
	fpkColumns   []string
	allColumns   []string
	queries      map[string]string
}

type TablePage struct {
	Start  int
	Size   int
	Filter interface{}
}

type sqlDB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// NewTable returns a table definition that can be used to manipulate a sql table.
// The supplied definition is a structure that represents the table and uses tags
// to define the columns:
// * column:"column name" Defines the name of the column in the database
// * pk:"true|generated" Defines a primary key.  Only one generated key is allowed
// * fk:"true" Defines a foreign key
func NewTable(schema string, tableName string, definition interface{}) (*TableDef, error) {
	// Base validation
	if strings.TrimSpace(tableName) == "" {
		return nil, ErrUndefinedTableName
	}

	// Find the underlying data type and check it's a structure
	e := reflect.ValueOf(definition)
	for ; e.Kind() == reflect.Interface || e.Kind() == reflect.Ptr; e = e.Elem() {
	}
	if e.Kind() != reflect.Struct {
		return nil, ErrNotAStructure
	}
	tableType := e.Type()

	// Use schema if provided
	tableName = wrapName(tableName)
	if strings.TrimSpace(schema) != "" {
		tableName = fmt.Sprintf("%s.%s", wrapName(strings.TrimSpace(schema)), strings.TrimSpace(tableName))
	}

	// Prepare the table metadata structure
	table := TableDef{
		name:       tableName,
		tableType:  tableType,
		columns:    make(map[string]*columnDef, tableType.NumField()),
		fkColumns:  make([]string, 0, tableType.NumField()),
		pkColumns:  make([]string, 0, tableType.NumField()),
		fpkColumns: make([]string, 0, tableType.NumField()),
		allColumns: make([]string, 0, tableType.NumField()),
	}

	if err := extractMetadata(tableType, &table); err != nil {
		return nil, err
	}

	table.queries = make(map[string]string)
	table.queries[insertQuery] = table.insertQuery()
	table.queries[selectOneQuery] = table.selectOneQuery()
	table.queries[selectAllQuery] = table.selectAllQuery()
	table.queries[selectAllPageQuery] = table.selectAllPagedQuery()
	//table.queries[selectForeignQuery] = table.selectForeignQuery()
	//table.queries[selectForeignPagedQuery] = table.selectForeignPagedQuery()
	table.queries[updateQuery] = table.updateQuery()
	table.queries[deleteQuery] = table.deleteQuery()

	return &table, nil
}

func extractMetadata(tableType reflect.Type, table *TableDef) error {
	var ok bool
	var err error
	var value string
	var first string

	// Extract column metadata
	for i := 0; i < tableType.NumField(); i++ {
		f := tableType.Field(i)
		defined := false
		column := columnDef{fieldName: f.Name}

		// Check if field is public
		if !f.IsExported() {
			log.Printf("field '%s' is not exported and won't be used.\n", f.Name)
			continue
		}

		// If column tag defined then use this for the name, else use the actual name
		columnName := wrapName(strings.ToLower(f.Name))
		if value, ok = f.Tag.Lookup("column"); ok {
			columnName = wrapName(value)
			defined = true
		}

		// Primary key
		if value, ok = f.Tag.Lookup("pk"); ok {
			if strings.EqualFold(value, "generated") {
				if reflect.Int < f.Type.Kind() || f.Type.Kind() > reflect.Uint64 {
					return ErrInvalidGenKey
				}
				column.primaryKey = true
				if table.generatedKey == "" {
					table.generatedKey = columnName
				} else {
					return ErrMultipleGeneratedPks
				}
			} else {
				column.primaryKey, err = strconv.ParseBool(value)
				if err != nil {
					return ErrInvalidPrimaryKey
				}
			}
			defined = true
		}

		// Foreign key
		if value, ok = f.Tag.Lookup("fk"); ok {
			column.foreignKey, err = strconv.ParseBool(value)
			if err != nil {
				return ErrInvalidForeignKey
			}
			defined = true
		}

		if !defined {
			log.Printf("field '%s' does not include any markers and will be skipped\n", column.fieldName)
			continue
		}

		if _, exists := table.columns[columnName]; exists {
			return ErrDuplicateColumn
		}

		if first == "" {
			first = columnName
		}

		table.columns[columnName] = &column
		table.allColumns = append(table.allColumns, columnName)
		if column.foreignKey && !column.primaryKey {
			table.fkColumns = append(table.fkColumns, columnName)
		}
		if column.primaryKey && !column.foreignKey {
			table.pkColumns = append(table.pkColumns, columnName)
		}
		if column.primaryKey && column.foreignKey {
			table.fpkColumns = append(table.fpkColumns, columnName)
		}
	}

	if len(table.columns) == 0 {
		return ErrNoColumnsDefined
	}

	if len(table.pkColumns) == 0 {
		table.pkColumns = append(table.pkColumns, first)
		column := table.columns[first]
		column.primaryKey = true
	}

	return nil
}

func wrapName(columnName string) string {
	return fmt.Sprintf("\"%s\"", columnName)
}

func (t *TableDef) Insert(db sqlDB, in interface{}) (sql.Result, error) {
	columnNames, columnValues, err := t.columnValues(in, t.allColumns)
	if err != nil {
		return nil, err
	}
	insertColumns := strings.Join(columnNames, ",")
	query := fmt.Sprintf(t.queries[insertQuery], insertColumns, updatePlaceHolders(columnNames))
	return db.Exec(query, columnValues...)
}

func (t *TableDef) SelectOne(db sqlDB, in interface{}) (*sql.Rows, error) {
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	_, columnValues, err := t.columnValues(in, allPkCols)
	if err != nil {
		return nil, err
	}

	return db.Query(t.queries[selectOneQuery], columnValues...)
}

func (t *TableDef) SelectAll(db sqlDB) (*sql.Rows, error) {
	return db.Query(t.queries[selectAllQuery])
}

func (t *TableDef) SelectAllPaged(db sqlDB, page *TablePage) (*sql.Rows, error) {
	start := page.Start * page.Size
	end := (page.Start + 1) * page.Size
	return db.Query(t.queries[selectAllPageQuery], start, end)
}

//func (t *TableDef) SelectByForeignKey(db sqlDB, in interface{}) (*sql.Rows, error) {
//	if t.queries[selectForeignQuery] == "" {
//		return nil, ErrUndefinedFK
//	}
//	allFkCols := append(t.fkColumns, t.fpkColumns...)
//	_, columnValues, err := t.columnValues(in, allFkCols)
//	if err != nil {
//		return nil, err
//	}
//
//	return db.Query(t.queries[selectForeignQuery], columnValues...)
//}
//
//func (t *TableDef) SelectByForeignKeyPaged(db sqlDB, in interface{}, page *TablePage) (*sql.Rows, error) {
//	if t.queries[selectForeignPagedQuery] == "" {
//		return nil, ErrUndefinedFK
//	}
//	allFkCols := append(t.fkColumns, t.fpkColumns...)
//	start := page.Start * page.Size
//	end := (page.Start + 1) * page.Size
//	_, columnValues, err := t.columnValues(in, allFkCols)
//	if err != nil {
//		return nil, err
//	}
//	return db.Query(t.queries[selectForeignPagedQuery], append(columnValues, start, end)...)
//}
//
func (t *TableDef) Update(db sqlDB, in interface{}) (sql.Result, error) {
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	_, columnValues, err := t.columnValues(in, allPkCols)
	if err != nil {
		return nil, err
	}
	return db.Exec(t.queries[updateQuery], columnValues...)
}

func (t *TableDef) Delete(db sqlDB, in interface{}) (sql.Result, error) {
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	_, columnValues, err := t.columnValues(in, allPkCols)
	if err != nil {
		return nil, err
	}
	return db.Exec(t.queries[deleteQuery], columnValues...)
}

func (t *TableDef) ExtractRow(rows *sql.Rows) (interface{}, error) {
	result := reflect.New(t.tableType)
	valuePtrs := make([]interface{}, len(t.allColumns))

	for i := range t.allColumns {
		valuePtrs[i] = result.Elem().Field(i).Addr().Interface()
	}

	err := rows.Scan(valuePtrs...)
	return result.Interface(), err
}

func (t *TableDef) ExtractRow2(rows *sql.Rows) (interface{}, error) {
	result := reflect.New(t.tableType)
	valuePtrs := make([]interface{}, len(t.allColumns))
	for i := range t.allColumns {
		var value interface{}
		valuePtrs[i] = &value
	}

	err := rows.Scan(valuePtrs...)

	s := result.Elem()
	for i, vp := range valuePtrs {
		if f := s.Field(i); f.IsValid() {
			t.setColumnValue(f, reflect.ValueOf(vp).Elem().Elem())
		}
	}

	return result.Interface(), err
}

func (t *TableDef) insertQuery() string {
	query := fmt.Sprintf(insertSQL, t.name)
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) selectOneQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	query := fmt.Sprintf(selectWhereSQL, allCols, t.name, wherePlaceHolders(allPkCols))
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) selectAllQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	query := fmt.Sprintf(selectAllSQL, allCols, t.name)
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) selectAllPagedQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	allFpkCols := strings.Join(append(t.fkColumns, t.pkColumns...), ",")
	query := fmt.Sprintf(selectAllPagedSQL, allCols, allFpkCols, t.name)
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) selectForeignQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	allFkCols := append(t.fkColumns, t.fpkColumns...)
	if len(allFkCols) == 0 {
		return ""
	}
	query := fmt.Sprintf(selectWhereSQL, allCols, t.name, wherePlaceHolders(allFkCols))
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) selectForeignPagedQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	allFpkCols := strings.Join(append(t.fkColumns, t.pkColumns...), ",")
	allFkCols := append(t.fkColumns, t.fpkColumns...)
	if len(allFkCols) == 0 {
		return ""
	}
	query := fmt.Sprintf(selectWherePagedSQL, allCols, allFpkCols, t.name, wherePlaceHolders(allFkCols))
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) updateQuery() string {
	allCols := strings.Join(t.allColumns, ",")
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	query := fmt.Sprintf(updateSQL, t.name, allCols, updatePlaceHolders(t.allColumns), wherePlaceHolders(allPkCols))
	log.Printf("Generated query: %s", query)
	return query
}

func (t *TableDef) deleteQuery() string {
	allPkCols := append(t.fpkColumns, t.pkColumns...)
	query := fmt.Sprintf(deleteSQL, t.name, wherePlaceHolders(allPkCols))
	log.Printf("Generated query: %s", query)
	return query
}

func wherePlaceHolders(cols []string) string {
	size := len(cols) * 5
	for i := 0; i < len(cols); i++ {
		size += len(cols[i]) + 4
	}

	b := strings.Builder{}
	b.Grow(size)
	for i := 0; i < len(cols); i++ {
		b.WriteString(fmt.Sprintf("%s=$%d AND ", cols[i], i+1))
	}
	return b.String()[:b.Len()-5]
}

func updatePlaceHolders(cols []string) string {
	b := strings.Builder{}
	b.Grow(len(cols) * 4)
	for i := 1; i <= len(cols); i++ {
		b.WriteString(fmt.Sprintf("$%d,", i))
	}
	return b.String()[:b.Len()-1]
}

func (t *TableDef) columnValues(in interface{}, columns []string) ([]string, []interface{}, error) {
	// Define all return columns and set to nil
	var columnNames []string
	var result []interface{}

	// Find the underlying data type and check for a structure
	e := reflect.ValueOf(in)
	for ; e.Kind() == reflect.Interface || e.Kind() == reflect.Ptr; e = e.Elem() {
	}
	if e.Kind() != reflect.Struct {
		log.Printf("supplied data is not a valid structure")
		return nil, nil, ErrNotAStructure
	}

	for _, columnName := range columns {
		if column, ok := t.columns[columnName]; ok {
			f := e.FieldByName(column.fieldName)
			if f.IsValid() {
				columnNames = append(columnNames, columnName)
				result = append(result, f.Interface())
			} else {
				log.Printf("structure does not contain field '%s'", column.fieldName)
			}
		} else {
			log.Printf("column name mapping '%s' does not exist", columnName)
		}
	}

	return columnNames, result, nil
}

func (t *TableDef) setColumnValue(f reflect.Value, rValue reflect.Value) bool {
	if !f.CanSet() {
		return false
	}

	switch f.Kind() {
	case reflect.Int:
		f.SetInt(rValue.Int())
	case reflect.Float32:
		var f32 float32 = 1.3
		x := reflect.ValueOf(f32)
		fmt.Sprintf("%v", x)
		var f64 float64 = rValue.Float()
		f.SetFloat(f64)
	case reflect.Float64:
		f.SetFloat(rValue.Float())
	default:
		log.Printf("Unsupported data type: %s", f.Kind())
	}

	return true
}
