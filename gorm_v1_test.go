package gorm_test

import (
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

	// Create sample records in TestModelV1 and RelatedModelV1
	testRecord := TestModel{Name: "Join Test"}
	db.Create(&testRecord)
	relatedRecord := RelatedModel{TestModelID: testRecord.ID, Description: "Related Description"}
	db.Create(&relatedRecord)

	// Perform join operation
	var results []struct {
		Name string
		Description   string
	}
	err = db.Table("test_models").Select("test_models.name, related_models.description").Joins("left join related_models on related_models.test_model_id = test_models.id").Scan(&results).Error
	if err != nil {
		t.Errorf("Joins method failed in GORM v1: %v", err)
	}

	if len(results) == 0 || results[0].Name != "Join Test" || results[0].Description != "Related Description" {
		t.Errorf("Joins method in GORM v1 did not return the correct data")
	}
}
