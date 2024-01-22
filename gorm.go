package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	gormV1 "github.com/jinzhu/gorm"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	gormV2 "gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)


var (
	// ErrRecordNotFound returns a "record not found error". Occurs only when attempting to query the database with a struct; querying with a slice won't return this error
	ErrRecordNotFound = gormV2.ErrRecordNotFound
	// ErrInvalidSQL occurs when you attempt a query with invalid SQL
	ErrInvalidSQL = gormV1.ErrInvalidSQL
	// ErrInvalidTransaction occurs when you are trying to `Commit` or `Rollback`
	ErrInvalidTransaction = gormV2.ErrInvalidTransaction
	// ErrCantStartTransaction can't start transaction when you are trying to start one with `Begin`
	ErrCantStartTransaction = gormV1.ErrCantStartTransaction
	// ErrUnaddressable unaddressable value
	ErrUnaddressable = gormV1.ErrUnaddressable
)

type DB struct {
	GormDB       *gormV2.DB
	Error        error
	RowsAffected int64
	Committed    bool
}

type Scope struct {
	gormV1.Scope
}

func (w *DB) Create(value interface{}) *DB {
	result := w.GormDB.Create(value)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Save(value interface{}) *DB {
	result := w.GormDB.Save(value)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Delete(value interface{}, where ...interface{}) *DB {
	result := w.GormDB.Delete(value, where...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Updates(values interface{}) *DB {
	result := w.GormDB.Updates(values)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) First(out interface{}, where ...interface{}) *DB {
	result := w.GormDB.First(out, where...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Take(out interface{}, where ...interface{}) *DB {
	result := w.GormDB.Take(out, where...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Last(out interface{}, where ...interface{}) *DB {
	result := w.GormDB.Last(out, where...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Find(out interface{}, where ...interface{}) *DB {
	result := w.GormDB.Find(out, where...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Update(column string, value interface{}) *DB {
	result := w.GormDB.Update(column, value)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) UpdateColumns(values interface{}) *DB {
	result := w.GormDB.UpdateColumns(values)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Model(value interface{}) *DB {
	return &DB{GormDB: w.GormDB.Model(value), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Where(query interface{}, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Where(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Or(query interface{}, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Or(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Not(query interface{}, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Not(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func ConvertToInt(value interface{}) (int, error) {
    // Check if value is of type int or a convertible type
    switch v := value.(type) {
    case int:
        // If it's already an int, return it as is
        return v, nil
    case int64:
        // If it's an int64, convert it to int and return
        if v > math.MaxInt || v < math.MinInt {
            return 0, errors.New("int64 value out of range for int")
        }
        return int(v), nil
    case float64:
        // If it's a float64, convert it to int and return
        if v != float64(int(v)) {
            return 0, errors.New("float64 value cannot be converted to int")
        }
        return int(v), nil
    default:
        return 0, errors.New("value is not convertible to int")
    }
}


func (w *DB) Limit(limit interface{}) *DB {
	limitInt, err := ConvertToInt(limit)
	if err != nil {
		return &DB{GormDB: w.GormDB.Limit(limitInt), Error: err, RowsAffected: w.RowsAffected}
	}
	return &DB{GormDB: w.GormDB.Limit(limitInt), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Offset(offset interface{}) *DB {
	offsetInt, err := ConvertToInt(offset)
	if err != nil {
		return &DB{GormDB: w.GormDB.Offset(offsetInt), Error: err, RowsAffected: w.RowsAffected}
	}
	return &DB{GormDB: w.GormDB.Offset(offsetInt), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Order(value interface{}) *DB {
	return &DB{GormDB: w.GormDB.Order(value), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Select(query interface{}, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Select(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Scopes(funcs ...func(*DB) *DB) *DB {
    modifiedFuncs := make([]func(*gormV2.DB) *gormV2.DB, len(funcs))

    for i, f := range funcs {
        // Create a closure that takes a *gormV2.DB and calls the function with the wrapped GormDB
        modifiedFuncs[i] = func(gdb *gormV2.DB) *gormV2.DB {
            return f(&DB{GormDB: gdb}).GormDB
        }
    }

    modifiedGormDB := w.GormDB.Scopes(modifiedFuncs...)

    return &DB{
        GormDB:       modifiedGormDB,
        Error:        w.Error,
        RowsAffected: w.RowsAffected,
        Committed:    w.Committed,
    }
}



func (w *DB) Preload(column string, conditions ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Preload(column, conditions...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Raw(sql string, values ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Raw(sql, values...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Exec(sql string, values ...interface{}) *DB {
	result := w.GormDB.Exec(sql, values...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Joins(query string, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Joins(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Group(query string) *DB {
	return &DB{GormDB: w.GormDB.Group(query), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Having(query string, args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Having(query, args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Distinct(args ...interface{}) *DB {
	return &DB{GormDB: w.GormDB.Distinct(args...), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Table(name string) *DB {
	return &DB{GormDB: w.GormDB.Table(name), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Debug() *DB {
	return &DB{GormDB: w.GormDB.Debug(), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Begin() *DB {
	return &DB{GormDB: w.GormDB.Begin(), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Commit() *DB {
    tx := w.GormDB.Commit()
    committed := tx.Error == nil // If there's no error, the transaction is considered committed.
    return &DB{GormDB: tx, Error: tx.Error, Committed: committed}
}

func (w *DB) Rollback() *DB {
	return &DB{GormDB: w.GormDB.Rollback(), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Transaction(fc func(tx *DB) error) (err error) {
	return w.GormDB.Transaction(func(tx *gormV2.DB) error {
		return fc(&DB{GormDB: tx})
	})
}

type Association struct {
    *gormV2.Association
}

func (w *DB) Association(column string) *Association {
	return &Association{w.GormDB.Association(column)}
}

func (assoc *Association) Count() int {
    count := int64(0)
    // Assuming you have a way to calculate or retrieve the count
    // For example, if using GORM's Count method:
    count = assoc.Association.Count()

    // Convert int64 to int
    return int(count)
}



func (w *DB) AutoMigrate(dst ...interface{}) *DB {
    err := w.GormDB.AutoMigrate(dst...)
    return &DB{GormDB: w.GormDB, Error: err, RowsAffected: w.RowsAffected}
}

func (w *DB) Unscoped() *DB {
	return &DB{GormDB: w.GormDB.Unscoped(), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Count(value interface{}) *DB {
    var count int64
    result := w.GormDB.Count(&count)
	err := result.Error

    switch val := value.(type) {
    case *int:
        *val = int(count)
    case *int64:
        *val = count
    default:
        err = errors.New("value must be a pointer to int or int64")
    }

    return &DB{
        GormDB:       w.GormDB,
        Error:        err,
        RowsAffected: result.RowsAffected,
        Committed:    w.Committed,
    }
}


func (w *DB) Pluck(column string, value interface{}) *DB {
	result := w.GormDB.Pluck(column, value)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Scan(dest interface{}) *DB {
	result := w.GormDB.Scan(dest)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Clone() *DB {
	return &DB{GormDB: w.GormDB.Session(&gormV2.Session{NewDB: true})}
}

func (w *DB) Related(value interface{}, foreignKeys ...string) *DB {
	result := w.GormDB.Model(value).Association(foreignKeys[0]).Find(value)
	return &DB{GormDB: w.GormDB, Error: result, RowsAffected: 0}
}

func (w *DB) AddForeignKey(field string, dest string, onDelete string, onUpdate string) *DB {
	// Note: GORM v2 handles foreign keys differently, this method is more of a placeholder
	// and should be adapted based on your specific database and GORM v2's constraints API.
	return w
}

func (w *DB) DropColumn(column string) *DB {
	result := w.GormDB.Migrator().DropColumn(w.GormDB.Statement.Table, column)
	return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
}

// In Gorm V1, ModifyColumn directly allows changing the type of a specific column by passing the column name and the new type as strings. In contrast, Gorm V2 uses a more structured approach with the Migrator().AlterColumn method, which typically operates based on the model's schema definition rather than direct string inputs for column types.
//
//To implement this in Gorm V2, you would typically need to:
//
//Ensure that your model struct reflects the desired column type change.
//Use Migrator().AlterColumn to apply this change to the database schema.
//This approach might require additional steps or adjustments in your adapter to align with the original Gorm V1 behavior, particularly if you want to allow dynamic type changes based on string inputs.
//
//Given these considerations, while it's possible to create a functional equivalent of V1's ModifyColumn in V2, it may not be a direct one-to-one mapping due to the differences in how the two versions of Gorm handle schema changes.
func (w *DB) ModifyColumn(value interface{}, column string) *DB {
    err := w.GormDB.Migrator().AlterColumn(value, column)
    return &DB{GormDB: w.GormDB, Error: err, RowsAffected: w.RowsAffected}
}


func (w *DB) DropTableIfExists(values ...interface{}) *DB {
	result := w.GormDB.Migrator().DropTable(values...)
	return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
}

func (w *DB) HasTable(value interface{}) bool {
	return w.GormDB.Migrator().HasTable(value)
}

func (w *DB) CreateTable(values ...interface{}) *DB {
	result := w.GormDB.Migrator().CreateTable(values...)
	return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
}

func (w *DB) RenameColumn(oldName string, newName string) *DB {
	result := w.GormDB.Migrator().RenameColumn(w.GormDB.Statement.Table, oldName, newName)
	return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
}

func (w *DB) RawSQL(sql string, values ...interface{}) *DB {
	// Note: RawSQL is not a direct GORM v1 method, but added here for executing raw SQL commands
	result := w.GormDB.Raw(sql, values...)
	return &DB{GormDB: w.GormDB, Error: result.Error, RowsAffected: result.RowsAffected}
}

// Deprecated: SingularTable is deprecated and will be removed in a future version.
func (w *DB) SingularTable(enable bool) {
	// In GORM v2, this setting is done at the GORM DB engine level, not on the DB instance.
	// This method here does not change state but is provided for compatibility.
}

func (w *DB) Set(key string, value interface{}) *DB {
	return &DB{GormDB: w.GormDB.Set(key, value), Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) Get(key string) (interface{}, bool) {
	return w.GormDB.Get(key)
}

// func (w *DB) Callback() *Callback
// this function doesn't map to gorm V2.  Use Gorm V2 methods for this functionality.

/*
func (w *DB) DB() *sql.DB {
	db, _ := w.GormDB.DB()
	return db
}
 */


func (w *DB) NewScope(value interface{}) *Statement {
    stmt := &gormV2.Statement{DB: w.GormDB, Model: value}
    return &Statement{Statement: stmt}
}

func (w *DB) CommonDB() gormV1.SQLCommon {
	db, err := w.GormDB.DB()
	if err != nil {
		// Handle the error, maybe return nil or log the issue
		return nil
	}
	return db
}

func (w *DB) DB() *sql.DB {
    db, err := w.GormDB.DB()
    if err != nil {
        // Handle the error, maybe return nil or log the issue
        return nil
    }
    return db
}

// Deprecated: NewRecord is not needed in gorm V2.
// The following is a dumbed down implementation that may not work.
// It will raise panic if the primary key name is not ID.
func (w *DB) NewRecord(value interface{}) bool {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// Check if the 'ID' field exists
	idVal := val.FieldByName("ID")
	if !idVal.IsValid() {
		panic("No 'ID' field found in the model")
	}

	// Assuming 'ID' is an integer field
	return idVal.Int() == 0
}


func (w *DB) RecordNotFound() bool {
	return errors.Is(w.Error, gormV2.ErrRecordNotFound)
}

func (w *DB) CreateTableIfNotExists(values ...interface{}) *DB {
	// This method is a combination of CreateTable and HasTable to replicate the v1 functionality.
	for _, value := range values {
		if !w.GormDB.Migrator().HasTable(value) {
			result := w.GormDB.Migrator().CreateTable(value)
			if result != nil {
				return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
			}
		}
	}
	return &DB{GormDB: w.GormDB, Error: nil, RowsAffected: w.RowsAffected}
}


func Open(dialect string, args ...interface{}) (*DB, error) {
    var dialector gormV2.Dialector

    switch dialect {
    case "sqlite3":
        dialector = sqlite.Open(args[0].(string))
    case "postgres":
        dialector = postgres.Open(args[0].(string))
    case "mysql":
        dialector = mysql.Open(args[0].(string))
    case "mssql":
        dialector = sqlserver.Open(args[0].(string))
    default:
        return nil, fmt.Errorf("unsupported dialect: %s", dialect)
    }

    db, err := gormV2.Open(dialector, &gormV2.Config{})
    return &DB{GormDB: db}, err
}


type Statement struct {
    *gormV2.Statement
}

func (s *Statement) Dialect() Dialect {
	return &commonDialect{Dialector: s.DB.Dialector}
}


func (s *Statement) TableName() string {
    // Logic to return the table name
    return s.Statement.Table
}

func (s *Statement) initializeField(schemaField *schema.Field) *Field {
    field := &Field{
        Field:        schemaField,
        IsForeignKey: s.isFieldForeignKey(schemaField),
    }

    // Set Relationship if applicable
    if rel := s.Statement.Schema.Relationships.Relations[schemaField.Name]; rel != nil {
        field.Relationship = extractRelationship(rel)
    }

    return field
}

func (s *Statement) FieldByName(name string) (*Field, bool) {
    if s.Statement == nil || s.Statement.Schema == nil {
        return nil, false
    }

    schemaField := s.Statement.Schema.LookUpField(name)
    if schemaField == nil {
        return nil, false
    }

    return s.initializeField(schemaField), true
}

func (s *Statement) GetStructFields() []*Field {
    if s.Statement == nil || s.Statement.Schema == nil {
        return nil
    }

    fields := make([]*Field, len(s.Statement.Schema.Fields))
    for i, schemaField := range s.Statement.Schema.Fields {
        fields[i] = s.initializeField(schemaField)
    }

    return fields
}

func (s *Statement) isFieldForeignKey(field *schema.Field) bool {
    for _, rel := range s.Statement.Schema.Relationships.Relations {
        for _, ref := range rel.References {
            if ref.ForeignKey != nil && ref.ForeignKey.Name == field.Name {
                return true
            }
        }
    }
    return false
}




type Field struct {
    *schema.Field
	StructField
    Relationship *Relationship
	IsForeignKey bool
}

type Relationship struct {
    Kind                           string
    ForeignFieldNames              []string
    AssociationForeignFieldNames   []string
}

func extractRelationship(rel *schema.Relationship) *Relationship {
    relationship := &Relationship{
        // Map GORM v2 relationship characteristics to GORM v1 style
        Kind: extractRelationshipKind(rel),
        // Populate these slices based on the GORM v2 relationship data
        ForeignFieldNames:              extractForeignFieldNames(rel),
        AssociationForeignFieldNames:   extractAssociationForeignFieldNames(rel),
    }
    return relationship
}

func extractRelationshipKind(rel *schema.Relationship) string {
    // In GORM v2, relationships are determined by the References field.
    // This function needs to interpret these references to deduce the relationship kind.

    if len(rel.References) == 0 {
        return "unknown"
    }

    // Checking for Belongs To relationship
    if rel.Type == schema.BelongsTo {
        return "belongs_to"
    }

    // Checking for Has One relationship
    if rel.Type == schema.HasOne {
        return "has_one"
    }

    // Checking for Has Many relationship
    if rel.Type == schema.HasMany {
        return "has_many"
    }

    // Checking for Many To Many relationship
    if rel.Type == schema.Many2Many {
        return "many_to_many"
    }

    return "unknown"
}


func extractForeignFieldNames(rel *schema.Relationship) []string {
    var fieldNames []string
    for _, ref := range rel.References {
        if ref.ForeignKey != nil {
            fieldNames = append(fieldNames, ref.ForeignKey.DBName)
        }
    }
    return fieldNames
}

func extractAssociationForeignFieldNames(rel *schema.Relationship) []string {
    var fieldNames []string
    for _, ref := range rel.References {
        if ref.PrimaryKey != nil {
            fieldNames = append(fieldNames, ref.PrimaryKey.DBName)
        }
    }
    return fieldNames
}

func (w *DB) DropTable(values ...interface{}) *DB {
    result := w.GormDB.Migrator().DropTable(values...)
    return &DB{GormDB: w.GormDB, Error: result, RowsAffected: w.RowsAffected}
}

func (w *DB) New() *DB {
    // Initialize a new gormV2.DB with the same configuration as the current one
    newDB := w.GormDB.Session(&gormV2.Session{})

    // Return a new instance of your custom DB struct
    return &DB{GormDB: newDB}
}

func (w *DB) RollbackUnlessCommitted() *DB {
    tx := w.Begin()
    if tx.Error != nil {
        return &DB{GormDB: w.GormDB, Error: tx.Error, Committed: tx.Committed}
    }
    if !tx.Committed {
        tx.Rollback()
    }
    return &DB{GormDB: tx.GormDB, Error: tx.Error, Committed: tx.Committed}
}



func (w *DB) LogMode(enable bool) *DB {
    var logLevel logger.LogLevel
    if enable {
        logLevel = logger.Info
    } else {
        logLevel = logger.Silent
    }

    newLogger := w.GormDB.Logger.LogMode(logLevel)
    w.GormDB.Session(&gormV2.Session{Logger: newLogger})
    return w
}

func (w *DB) SetMaxIdleConns(maxIdleConns int) {
    sqlDB, err := w.GormDB.DB()
    if err != nil {
        // Handle error, maybe log it or set w.Error
        return
    }
    sqlDB.SetMaxIdleConns(maxIdleConns)
}

func (w *DB) GetSQLDB() (*sql.DB, error) {
    sqlDB, err := w.GormDB.DB()
    return sqlDB, err
}

func (w *DB) FirstOrCreate(dest interface{}, conds ...interface{}) *DB {
    result := w.GormDB.FirstOrCreate(dest, conds...)
    return &DB{GormDB: result.Statement.DB, Error: result.Error, RowsAffected: result.RowsAffected}
}

func (w *DB) Assign(attrs ...interface{}) *DB {
    result := w.GormDB.Assign(attrs...)
    return &DB{GormDB: result, Error: w.Error, RowsAffected: w.RowsAffected}
}

func (w *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) *DB {
    tx := w.GormDB.WithContext(ctx).Begin(opts)
    return &DB{GormDB: tx, Error: tx.Error}
}

func (w *DB) Row() *sql.Row {
    return w.GormDB.Session(&gormV2.Session{}).Limit(1).Find(nil).Row()
}

func (w *DB) Rows() (*sql.Rows, error) {
    return w.GormDB.Session(&gormV2.Session{}).Find(nil).Rows()
}

func (w *DB) ScanRows(rows *sql.Rows, dest interface{}) error {
    // Check if rows is nil
    if rows == nil {
        return errors.New("rows is nil")
    }

    // Ensure rows are closed after processing
    defer rows.Close()

    // Get the column types for scanning
    columns, err := rows.Columns()
    if err != nil {
        return err
    }

    // Create a slice of interfaces to hold column values
    values := make([]interface{}, len(columns))
    for i := range values {
        var v interface{}
        values[i] = &v
    }

    // Iterate through each row and scan it into the destination
    for rows.Next() {
        err := rows.Scan(values...)
        if err != nil {
            return err
        }

        // Manually map the scanned values to the destination struct
        // This part needs to be adjusted based on the structure of 'dest'
        // For example, if 'dest' is a slice of structs, you will need to
        // create a new struct for each row, map the values, and append it to the slice.
    }

    // Check for errors encountered during iteration over the rows
    return rows.Err()
}

type SqlExpr struct {
    SQL  string
    Args []interface{}
}

func (w *DB) QueryExpr() *SqlExpr {
    // Prepare the query without executing it using DryRun
    stmt := w.GormDB.Session(&gormV2.Session{DryRun: true}).Find(nil).Statement

    // In case of an error during the preparation, return an empty SqlExpr
    if stmt.Error != nil {
        return &SqlExpr{}
    }

    // Return the SQL string and its arguments encapsulated in an SqlExpr
    return &SqlExpr{
        SQL:  stmt.SQL.String(),
        Args: stmt.Vars,
    }
}

func (w *DB) Omit(columns ...string) *DB {
    // Apply the Omit configuration to the underlying GormDB
    newDB := w.GormDB.Omit(columns...)

    // Return a new instance of your DB struct with the updated GormDB
    return &DB{
        GormDB:       newDB,
        Error:        w.Error,
        RowsAffected: w.RowsAffected,
        Committed:    w.Committed,
    }
}

func getDialectName(dialector gormV2.Dialector) string {
    switch dialector.(type) {
    case *mysql.Dialector:
        return "mysql"
    case *postgres.Dialector:
        return "postgres"
    case *sqlite.Dialector:
        return "sqlite"
    case *sqlserver.Dialector:
        return "sqlserver"
    // Add other dialects as needed
    default:
        return "unknown"
    }
}

func (w *DB) Dialect() Dialect {
    return &commonDialect{Dialector: w.GormDB.Dialector}
}

type commonDialect struct {
	//db SQLCommon
    Dialector gormV2.Dialector
	DefaultForeignKeyNamer
}

func (d *commonDialect) HasIndex(tableName string, indexName string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) HasForeignKey(tableName string, foreignKeyName string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) RemoveIndex(tableName string, indexName string) error {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) HasTable(tableName string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) HasColumn(tableName string, columnName string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) ModifyColumn(tableName string, columnName string, typ string) error {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) LimitAndOffsetSQL(limit, offset interface{}) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) CurrentDatabase() string {
	//TODO implement me
	panic("implement me")
}

func (d *commonDialect) GetName() string {
    return getDialectName(d.Dialector)
}

type Model struct {
	*gormV2.Model
}

// Dialect interface contains behaviors that differ across SQL database
type Dialect interface {
	// GetName get dialect's name
	GetName() string

	// SetDB set db for dialect
	//SetDB(db SQLCommon)

	// BindVar return the placeholder for actual values in SQL statements, in many dbs it is "?", Postgres using $1
	BindVar(i int) string
	// Quote quotes field name to avoid SQL parsing exceptions by using a reserved word as a field name
	Quote(key string) string
	// DataTypeOf return data's sql type
	//DataTypeOf(field *StructField) string

	// HasIndex check has index or not
	HasIndex(tableName string, indexName string) bool
	// HasForeignKey check has foreign key or not
	HasForeignKey(tableName string, foreignKeyName string) bool
	// RemoveIndex remove index
	RemoveIndex(tableName string, indexName string) error
	// HasTable check has table or not
	HasTable(tableName string) bool
	// HasColumn check has column or not
	HasColumn(tableName string, columnName string) bool
	// ModifyColumn modify column's type
	ModifyColumn(tableName string, columnName string, typ string) error

	// LimitAndOffsetSQL return generated SQL with Limit and Offset, as mssql has special case
	LimitAndOffsetSQL(limit, offset interface{}) (string, error)
	// SelectFromDummyTable return select values, for most dbs, `SELECT values` just works, mysql needs `SELECT value FROM DUAL`
	SelectFromDummyTable() string
	// LastInsertIDOutputInterstitial most dbs support LastInsertId, but mssql needs to use `OUTPUT`
	LastInsertIDOutputInterstitial(tableName, columnName string, columns []string) string
	// LastInsertIdReturningSuffix most dbs support LastInsertId, but postgres needs to use `RETURNING`
	LastInsertIDReturningSuffix(tableName, columnName string) string
	// DefaultValueStr
	DefaultValueStr() string

	// BuildKeyName returns a valid key name (foreign key, index key) for the given table, field and reference
	BuildKeyName(kind, tableName string, fields ...string) string

	// NormalizeIndexAndColumn returns valid index name and column name depending on each dialect
	NormalizeIndexAndColumn(indexName, columnName string) (string, string)

	// CurrentDatabase return current database name
	CurrentDatabase() string
}

var keyNameRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

// DefaultForeignKeyNamer contains the default foreign key name generator method
type DefaultForeignKeyNamer struct {
}


//func init() {
	//RegisterDialect("common", &commonDialect{})
//}

//func (s *commonDialect) SetDB(db SQLCommon) {
	//s.db = db
//}

func (commonDialect) BindVar(i int) string {
	return "$$$" // ?
}

func (commonDialect) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

//func (s *commonDialect) fieldCanAutoIncrement(field *StructField) bool {
	//if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		//return strings.ToLower(value) != "false"
	//}
	//return field.IsPrimaryKey
//}

type StructField struct {
	gormV1.StructField
}

/*
func (s commonDialect) HasIndex(tableName string, indexName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ? AND index_name = ?", currentDatabase, tableName, indexName).Scan(&count)
	return count > 0
}

func (s commonDialect) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	return err
}

func (s commonDialect) HasForeignKey(tableName string, foreignKeyName string) bool {
	return false
}

func (s commonDialect) HasTable(tableName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?", currentDatabase, tableName).Scan(&count)
	return count > 0
}

func (s commonDialect) HasColumn(tableName string, columnName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND column_name = ?", currentDatabase, tableName, columnName).Scan(&count)
	return count > 0
}

func (s commonDialect) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v", tableName, columnName, typ))
	return err
}

func (s commonDialect) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	return
}

// LimitAndOffsetSQL return generated SQL with Limit and Offset
func (s commonDialect) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {
	if limit != nil {
		if parsedLimit, err := s.parseInt(limit); err != nil {
			return "", err
		} else if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)
		}
	}
	if offset != nil {
		if parsedOffset, err := s.parseInt(offset); err != nil {
			return "", err
		} else if parsedOffset >= 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		}
	}
	return
}

 */

func (commonDialect) SelectFromDummyTable() string {
	return ""
}

func (commonDialect) LastInsertIDOutputInterstitial(tableName, columnName string, columns []string) string {
	return ""
}

func (commonDialect) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}

func (commonDialect) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

// BuildKeyName returns a valid key name (foreign key, index key) for the given table, field and reference
func (DefaultForeignKeyNamer) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := fmt.Sprintf("%s_%s_%s", kind, tableName, strings.Join(fields, "_"))
	keyName = keyNameRegex.ReplaceAllString(keyName, "_")
	return keyName
}

// NormalizeIndexAndColumn returns argument's index name and column name without doing anything
func (commonDialect) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	return indexName, columnName
}

func (commonDialect) parseInt(value interface{}) (int64, error) {
	return strconv.ParseInt(fmt.Sprint(value), 0, 0)
}

// IsByteArrayOrSlice returns true of the reflected value is an array or slice
func IsByteArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Type().Elem() == reflect.TypeOf(uint8(0))
}

func Expr(expression string, args ...interface{}) *SqlExpr {
	return &SqlExpr{SQL: expression, Args: args}
}