package gorm_test
import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	gormV1 "github.com/jinzhu/gorm"
	"testing"
)

func TestRawSQLGormV1(t *testing.T) {
	db, err := gormV1.Open("mysql", "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// AutoMigrate to create the test_models table
	if err := db.AutoMigrate(&TestModel{}).Error; err != nil {
		t.Fatalf("Failed to auto-migrate: %v", err)
	}

	defer db.Exec("DROP TABLE test_models;")

	// Create a record for testing
	expectedName := "RawSQL Test V1"
	if err := db.Create(&TestModel{Name: expectedName}).Error; err != nil {
		t.Fatalf("Failed to create test record: %v", err)
	}

	// Execute a raw SQL query
	var model TestModel
	if err := db.Raw("SELECT * FROM test_models WHERE name = ?", expectedName).Scan(&model).Error; err != nil {
		t.Errorf("RawSQL method failed in GORM v1: %v", err)
	}

	if model.Name != expectedName {
		t.Errorf("RawSQL method in GORM v1 did not return the correct data, expected %s, got %s", expectedName, model.Name)
	}
}

func TestJoinsGormV1(t *testing.T) {
	db, err := gormV1.Open("mysql", "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// AutoMigrate to create the tables
	if err := db.AutoMigrate(&TestModel{}, &RelatedModel{}).Error; err != nil {
		t.Fatalf("Failed to auto-migrate: %v", err)
	}
	defer db.Exec("DROP TABLE test_models;")
	defer db.Exec("DROP TABLE related_models;")

	// Create sample records in TestModelV1 and RelatedModelV1
	testRecord := TestModel{Name: "Join Test"}
	db.Create(&testRecord)
	relatedRecord := RelatedModel{TestModelID: testRecord.ID, Description: "Related Description"}
	db.Create(&relatedRecord)

	// Perform join operation
	var results []struct {
		Name        string
		Description string
	}
	err = db.Table("test_models").Select("test_models.name, related_models.description").Joins("left join related_models on related_models.test_model_id = test_models.id").Scan(&results).Error
	if err != nil {
		t.Errorf("Joins method failed in GORM v1: %v", err)
	}

	if len(results) == 0 || results[0].Name != "Join Test" || results[0].Description != "Related Description" {
		t.Errorf("Joins method in GORM v1 did not return the correct data")
	}
}

func TestScanRowsV1(t *testing.T) {
	// Setup DB using GORM v1
	db, err := gormV1.Open("mysql", "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Migrate
	db.AutoMigrate(&TestModel{})
	defer db.Exec("DROP TABLE test_models;")

	// Create sample records
	for i := 0; i < 5; i++ {
		db.Create(&TestModel{Name: fmt.Sprintf("Test %d", i)})
	}

	// Perform raw SQL query
	rows, err := db.Raw("SELECT * FROM test_models").Rows()
	if err != nil {
		t.Fatalf("Failed to execute raw query: %v", err)
	}
	defer rows.Close()

	var models []TestModel
	for rows.Next() {
		var model TestModel
		if err := db.ScanRows(rows, &model); err != nil {
			t.Errorf("Failed to scan row: %v", err)
		}
		models = append(models, model)
	}

	if len(models) != 5 {
		t.Errorf("Expected 5 models, got %d", len(models))
	}
}

func TestScanRowsSimplified(t *testing.T) {
	db, err := gormV1.Open("mysql", "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()
	db.AutoMigrate(&TestModel{})
	defer db.Exec("DROP TABLE test_models;")

	// Enable detailed logging
	db.LogMode(true)

	// Create a test record
	testRecord := TestModel{Name: "Simplified Test", Category: "Test"}
	if err := db.Create(&testRecord).Error; err != nil {
		t.Fatalf("Failed to create test record: %v", err)
	}

	// Ensure the record was created
	if testRecord.ID == 0 {
		t.Fatalf("Test record was not created properly")
	}

	// Perform raw SQL query
	rows, err := db.Raw("SELECT * FROM test_models WHERE id = ?", testRecord.ID).Rows()
	if err != nil {
		t.Fatalf("Failed to execute raw SQL query: %v", err)
	}
	defer rows.Close()

	// Use GORM's ScanRows
	var model TestModel
	if rows.Next() {
		if err := db.ScanRows(rows, &model); err != nil {
			t.Errorf("ScanRows failed: %v", err)
		} else if model.ID != testRecord.ID || model.Name != testRecord.Name || model.Category != testRecord.Category {
			t.Errorf("ScanRows did not return the correct data: expected ID=%d, Name=%s, Category=%s; got ID=%d, Name=%s, Category=%s", testRecord.ID, testRecord.Name, testRecord.Category, model.ID, model.Name, model.Category)
		}
	} else {
		t.Errorf("No rows returned from query")
	}
}

func TestScanRowsV1a(t *testing.T) {
	// Setup DB using GORM v1
	db, err := gormV1.Open("mysql", "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Migrate
	db.AutoMigrate(&TestModel{})
	defer db.Exec("DROP TABLE test_models;")

	// Create records
	for i := 0; i < 5; i++ {
		db.Create(&TestModel{Name: fmt.Sprintf("Test %d", i)})
	}

	// Perform raw SQL query
	rows, err := db.Raw("SELECT * FROM test_models").Rows()
	if err != nil {
		t.Fatalf("Failed to execute raw query: %v", err)
	}
	defer rows.Close()

	// Get columns and log their number right after opening rows
	columns, colErr := rows.Columns()
	if colErr != nil {
		t.Errorf("Failed to get columns: %v", colErr)
	}
	t.Logf("Number of columns: %d", len(columns))

	var models []TestModel
	var rowCount int
	for rows.Next() {
		rowCount++
		var model TestModel
		if err := db.ScanRows(rows, &model); err != nil {
			t.Errorf("Failed to scan row: %v", err)
		} else {
			t.Logf("Scanned row %d: %+v", rowCount, model)
		}
		models = append(models, model)
	}

	if rowCount != 5 {
		t.Errorf("Expected 5 iterations in rows.Next(), got %d", rowCount)
	}

	if len(models) != 5 {
		t.Errorf("Expected 5 models, got %d", len(models))
	}

	if err := rows.Err(); err != nil {
		t.Errorf("Rows had an error: %v", err)
	}
}