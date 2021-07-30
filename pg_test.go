package sqldb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateTable(t *testing.T) {
	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	jsonFile, err := os.Open("test_table.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var data Table
	json.Unmarshal([]byte(byteValue), &data)

	CreateTable(data)

	tbl := GetSchema(data.Name)
	if len(tbl.Columns) == 0 {
		t.Errorf("Create table failed")
	}
}

func TestAddColumn(t *testing.T) {

	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	old := GetSchema("test")
	AddColumn("test", "addcolumn", "integer")
	new := GetSchema("test")

	if len(old.Columns) == len(new.Columns) {
		t.Errorf("Column already exist")
	}
}

func TestInsert(t *testing.T) {

	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	vl := make(AssRow)
	vl["name"] = "toto"
	vl["description"] = "tata"

	old := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	UpdateOrInsert("test", vl)

	new := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if len(jsonStringOld) == len(jsonStringNew) {
		t.Errorf("Error row not created")
	}
}

func TestUpdate(t *testing.T) {

	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	vl := make(AssRow)
	vl["id"] = 1
	vl["name"] = "titi"
	vl["description"] = "toto"

	old := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	UpdateOrInsert("test", vl)

	new := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if string(jsonStringOld) == string(jsonStringNew) {
		t.Errorf("Error row not updated")
	}

}

func TestDelete(t *testing.T) {

	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	vl := make(AssRow)
	vl["id"] = 1

	old := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	Delete("test", vl)

	new := GetAssociativeArray("test", []string{"*"}, "", []string{}, "")
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if len(jsonStringOld) == len(jsonStringNew) {
		t.Errorf("Error row not deleted")
	}
}

func TestDeleteColumn(t *testing.T) {

	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	old := GetSchema("test")
	DeleteColumn("test", "addcolumn")
	new := GetSchema("test")

	if len(old.Columns) == len(new.Columns) {
		t.Errorf("Error column not deleted")
	}
}

func TestDeleteTable(t *testing.T) {
	Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer Close()

	DeleteTable("test")

	tbl := GetSchema("test")

	if len(tbl.Columns) != 0 {
		t.Errorf("Delete table failed")
	}
}
