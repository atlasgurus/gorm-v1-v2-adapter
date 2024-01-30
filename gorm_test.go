package gorm_test

import (
    "fmt"
    "github.com/atlasgurus/gorm-v1-v2-adapter/gorm"
    "testing"
)

const mysqlConnectionString = "root:@tcp(localhost:3306)/testdb?charset=utf8&parseTime=True&loc=Local"

type TestModel struct {
    ID       uint
    Name     string
    Category string
}


func setupTestDB(t *testing.T) *gorm.DB {
    // Open database using your adapter's Open function
    adapterDB, err := gorm.Open("mysql", mysqlConnectionString)
    if err != nil {
        t.Fatalf("Failed to connect to the database using adapter: %v", err)
    }

    // AutoMigrate your test models using your adapter
    adapterDB.AutoMigrate(&TestModel{})
    adapterDB.AutoMigrate(&RelatedModel{})

    return adapterDB
}


func TestCreate(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    testRecord := TestModel{Name: "Test Record"}

    result := db.Create(&testRecord)
    if result.Error != nil {
        t.Errorf("Create method failed: %v", result.Error)
    }

    if result.RowsAffected != 1 {
        t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
    }

    // Additional checks can be added here to verify the record was created correctly
}

func TestFind(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    createdRecord := TestModel{Name: "Find Test"}
    db.Create(&createdRecord)

    var foundRecord TestModel
    findResult := db.Find(&foundRecord, "name = ?", "Find Test")
    if findResult.Error != nil {
        t.Errorf("Find method failed: %v", findResult.Error)
    }

    if foundRecord.Name != "Find Test" {
        t.Errorf("Expected to find record with name 'Find Test', found '%s'", foundRecord.Name)
    }

    // Additional checks can be added here to verify the retrieved record
}

func TestUpdate(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create a record to update
    originalRecord := TestModel{Name: "Original Name"}
    db.Create(&originalRecord)

    // Update the record
    updatedRecord := TestModel{Name: "Updated Name"}
    updateResult := db.Model(&originalRecord).Update("name", updatedRecord.Name)
    if updateResult.Error != nil {
        t.Errorf("Update method failed: %v", updateResult.Error)
    }

    // Fetch the updated record to verify
    var fetchedRecord TestModel
    db.First(&fetchedRecord, originalRecord.ID)
    if fetchedRecord.Name != updatedRecord.Name {
        t.Errorf("Record was not updated correctly, expected name %s, found %s", updatedRecord.Name, fetchedRecord.Name)
    }
}

func TestDelete(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create a record to delete
    recordToDelete := TestModel{Name: "Delete Me"}
    db.Create(&recordToDelete)

    // Delete the record
    deleteResult := db.Delete(&TestModel{}, recordToDelete.ID)
    if deleteResult.Error != nil {
        t.Errorf("Delete method failed: %v", deleteResult.Error)
    }

    // Attempt to find the deleted record
    var foundRecord TestModel
    findResult := db.First(&foundRecord, recordToDelete.ID)
    if !findResult.RecordNotFound() {
        t.Errorf("Record was not deleted")
    }
}

func TestTransaction(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Start a transaction
    tx := db.Begin()
    if tx.Error != nil {
        t.Fatalf("Failed to begin transaction: %v", tx.Error)
    }

    // Perform operations within the transaction
    record := TestModel{Name: "Transacted Record"}
    if tx.Create(&record).Error != nil {
        tx.Rollback()
        t.Fatalf("Create operation within transaction failed")
    }

    // Commit the transaction
    if tx.Commit().Error != nil {
        t.Errorf("Failed to commit transaction")
    }

    // Verify the record was created
    var foundRecord TestModel
    if db.First(&foundRecord, record.ID).Error != nil {
        t.Errorf("Record not found after transaction commit")
    }
}

func TestCount(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample records
    db.Create(&TestModel{Name: "Test 1"})
    db.Create(&TestModel{Name: "Test 2"})

    // Count the records
    var count int64
    countResult := db.Model(&TestModel{}).Count(&count)
    if countResult.Error != nil {
        t.Errorf("Count method failed: %v", countResult.Error)
    }

    if count != 2 {
        t.Errorf("Expected count to be 2, got %d", count)
    }
}

func TestPluck(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create a record
    db.Create(&TestModel{Name: "Pluck Test"})

    // Pluck the 'name' field
    var names []string
    pluckResult := db.Model(&TestModel{}).Pluck("name", &names)
    if pluckResult.Error != nil {
        t.Errorf("Pluck method failed: %v", pluckResult.Error)
    }

    if len(names) != 1 || names[0] != "Pluck Test" {
        t.Errorf("Pluck method did not retrieve the correct data")
    }
}

func TestScopes(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample records
    db.Create(&TestModel{Name: "Scope Test 1"})
    db.Create(&TestModel{Name: "Scope Test 2"})
    db.Create(&TestModel{Name: "Another Test"})

    // Define a scope that filters records by name
    nameScope := func(db *gorm.DB) *gorm.DB {
        return db.Where("name LIKE ?", "%Scope Test%")
    }

    // Apply the scope and fetch results
    var models []TestModel
    scopeResult := db.Scopes(nameScope).Find(&models)
    if scopeResult.Error != nil {
        t.Errorf("Scopes method failed: %v", scopeResult.Error)
    }

    if len(models) != 2 {
        t.Errorf("Expected 2 records, got %d", len(models))
    }
}


func TestOrder(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample records
    db.Create(&TestModel{Name: "C"})
    db.Create(&TestModel{Name: "A"})
    db.Create(&TestModel{Name: "B"})

    // Fetch records ordered by name
    var models []TestModel
    orderResult := db.Order("name").Find(&models)
    if orderResult.Error != nil {
        t.Errorf("Order method failed: %v", orderResult.Error)
    }

    if len(models) != 3 || models[0].Name != "A" || models[1].Name != "B" || models[2].Name != "C" {
        t.Errorf("Order method did not sort records correctly")
    }
}


func TestGroupAndHaving(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample records with a 'category' field
    db.Create(&TestModel{Name: "Test 1", Category: "A"})
    db.Create(&TestModel{Name: "Test 2", Category: "A"})
    db.Create(&TestModel{Name: "Test 3", Category: "B"})

    // Group records by category and filter with having
    var results []struct {
        Category string
        Count    int
    }
    groupResult := db.Model(&TestModel{}).Select("category, COUNT(*) as count").Group("category").Having("COUNT(*) > ?", 1).Scan(&results)
    if groupResult.Error != nil {
        t.Errorf("Group and Having methods failed: %v", groupResult.Error)
    }

    // Assuming we're expecting to find only one category with more than one record
    if len(results) != 1 || results[0].Category != "A" || results[0].Count != 2 {
        t.Errorf("Group and Having methods did not filter records correctly, got %#v", results)
    }
}

type RelatedModel struct {
    ID          uint
    TestModelID uint
    Description string
}

func TestJoins(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")
    defer db.GormDB.Exec("DROP TABLE related_models;")

    // Create sample records in TestModel and RelatedModel
    testRecord := TestModel{Name: "Join Test"}
    db.Create(&testRecord)
    relatedRecord := RelatedModel{TestModelID: testRecord.ID, Description: "Related Description"}
    db.Create(&relatedRecord)

    // Perform join operation
    var results []struct {
        Name string
        Description   string
    }
    joinResult := db.Table("test_models").Select("test_models.name, related_models.description").Joins("left join related_models on related_models.test_model_id = test_models.id").Scan(&results)
    if joinResult.Error != nil {
        t.Errorf("Joins method failed: %v", joinResult.Error)
    }

    if len(results) == 0 || results[0].Name != "Join Test" || results[0].Description != "Related Description" {
        t.Errorf("Joins method did not return the correct data")
    }
}

func TestNot(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample records
    db.Create(&TestModel{Name: "Not Test 1"})
    db.Create(&TestModel{Name: "Not Test 2"})
    db.Create(&TestModel{Name: "Another Test"})

    // Fetch records excluding a specific name
    var models []TestModel
    notResult := db.Not("name", "Another Test").Find(&models)
    if notResult.Error != nil {
        t.Errorf("Not method failed: %v", notResult.Error)
    }

    if len(models) != 2 {
        t.Errorf("Expected 2 records, got %d", len(models))
    }
}

func TestRawSQL(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

    // Create sample record
    db.Create(&TestModel{Name: "RawSQL Test"})

    // Execute a raw SQL query
    var model TestModel
    rawSQLResult := db.Raw("SELECT * FROM test_models WHERE name = ?", "RawSQL Test").Scan(&model)
    if rawSQLResult.Error != nil {
        t.Errorf("RawSQL method failed: %v", rawSQLResult.Error)
    }

    if model.Name != "RawSQL Test" {
        t.Errorf("RawSQL method did not return the correct data")
    }
}




func TestScanRows(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

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

func TestScanRowsA(t *testing.T) {
    db := setupTestDB(t)
    defer db.GormDB.Exec("DROP TABLE test_models;")

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

    var models []TestModel // Use a slice of TestModel
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
