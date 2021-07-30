package sqldb

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

var db *sql.DB

// AssRow : associative row type
type AssRow map[string]interface{}

// Table is a table structure description
type Table struct {
	Name    string            `json:"name"`
	Columns map[string]string `json:"columns"`
}

// Open the database
func Open(driver string, url string) {
	var err error
	db, err = sql.Open(driver, url)
	if err != nil {
		log.Println(err)
	}
}

// Close the database connection
func Close() {
	db.Close()
}

// GetAssociativeArray : Provide results as an associative array
func GetAssociativeArray(table string, columns []string, restriction string, sortkeys []string, dir string) []AssRow {
	return QueryAssociativeArray(buildSelect(table, "", columns, restriction, sortkeys, dir))
}

// QueryAssociativeArray : Provide results as an associative array
func QueryAssociativeArray(query string) []AssRow {
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		log.Println(query)
	}
	defer rows.Close()

	var results []AssRow
	cols, _ := rows.Columns()

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {

		}
		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(AssRow)
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = *val
		}
		// jsonString, _ := json.Marshal(m)
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		// fmt.Println(string(jsonString))

		results = append(results, m)

	}
	return results
}

// GetSchema : Provide results as an associative array
func GetSchema(table string) Table {
	t := Table{Name: table}
	cols := QueryAssociativeArray("SELECT column_name :: varchar as name, REPLACE(REPLACE(data_type,'character varying','varchar'),'character','char') || COALESCE('(' || character_maximum_length || ')', '') as type from INFORMATION_SCHEMA.COLUMNS where table_name ='" + table + "';")
	t.Columns = make(map[string]string)
	for _, row := range cols {
		var name, rowtype string
		for key, element := range row {
			if key == "name" {
				name = fmt.Sprintf("%v", element)
			}
			if key == "type" {
				rowtype = fmt.Sprintf("%v", element)
			}
		}
		t.Columns[name] = rowtype
	}
	return t
}

func ListTables() []AssRow {
	return QueryAssociativeArray("SELECT table_name :: varchar FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
}

func CreateTable(t Table) {
	query := "create table " + t.Name + " ( "
	columns := ""
	for name, rowtype := range t.Columns {
		if fmt.Sprintf("%v", name) == "id" {
			columns += fmt.Sprintf("%v", name) + " " + "SERIAL PRIMARY KEY,"
		} else {

			columns += fmt.Sprintf("%v", name) + " " + fmt.Sprintf("%v", rowtype)
			columns += ","
		}
	}
	query += columns
	query = query[:len(query)-1] + " )"
	_, err := db.Query(query)
	if err != nil {
		log.Println(query)
	}
	query = "create sequence if not exists sq_" + t.Name
	_, err = db.Query(query)
	if err != nil {
		log.Println(query)
	}
}

func DeleteTable(table string) {
	query := "drop table " + table
	_, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	query = "drop sequence if exists sq_" + table
	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}
}

func AddColumn(table string, name string, sqltype string) {
	query := "alter table " + table + " add " + name + " " + sqltype
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
}

func DeleteColumn(table string, name string) {
	query := "alter table " + table + " drop " + name
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
}

func ListSequences() []AssRow {
	return QueryAssociativeArray("SELECT sequence_name :: varchar FROM information_schema.sequences WHERE sequence_schema = 'public' ORDER BY sequence_name;")
}

func buildSelect(table string, key string, columns []string, restriction string, sortkeys []string, dir ...string) string {
	if key != "" {
		columns = append(columns, key)
	}
	query := "select " + strings.Join(columns, ",") + " from " + table
	if restriction != "" {
		query += " where " + restriction
	}
	if len(sortkeys) > 0 {
		query += " order by " + strings.Join(sortkeys, ",")
	}
	if len(dir) > 0 {
		query += " " + dir[0]
	}
	return query
}

func Insert(table string, record AssRow) int {
	columns := ""
	values := ""
	schema := GetSchema(table)
	var id int

	for key, element := range record {

		if strings.Contains(schema.Columns[key], "char") || strings.Contains(schema.Columns[key], "date") {
			columns += key + ","
			values += fmt.Sprintf(pq.QuoteLiteral(fmt.Sprintf("%v", element))) + ","
		} else {

			columns += key + ","
			values += fmt.Sprintf("%v", element) + ","
		}
	}

	db.QueryRow("INSERT INTO " + table + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ") RETURNING id").Scan(&id)
	return id
}

func Update(table string, record AssRow) string {

	schema := GetSchema(table)
	id := ""
	stack := ""

	for key, element := range record {

		if strings.Contains(schema.Columns[key], "char") || strings.Contains(schema.Columns[key], "date") {

			stack = stack + " " + key + " = " + pq.QuoteLiteral(fmt.Sprintf("%v", element)) + ","

		} else {

			if key == "id" {
				id = fmt.Sprintf("%v", element)
			} else {
				stack = stack + " " + key + " = " + fmt.Sprintf("%v", element) + ","
			}
		}
	}
	stack = removeLastChar(stack)
	query := ("UPDATE " + table + " SET " + stack + " WHERE id = " + id)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(query)
		log.Println(err)
	}
	defer rows.Close()
	return query
}

func Delete(table string, record AssRow) string {
	id := ""
	values := ""

	for key, element := range record {
		if key == "id" {
			values += fmt.Sprintf("%v", element) + ","
			id = removeLastChar(values)

		}
	}
	query := ("DELETE FROM " + table + " WHERE id = " + id)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(query)
		log.Println(err)
	}
	defer rows.Close()
	return query
}

func UpdateOrInsert(table string, record AssRow) int {
	id := 0

	for key, element := range record {
		if key == "id" {
			sid := fmt.Sprintf("%v", element)
			id, _ = strconv.Atoi(sid)
		}
	}
	if id == 0 {
		return Insert(table, record)
	} else {
		Update(table, record)
		return id
	}
}

func removeLastChar(s string) string {
	r := []rune(s)
	return string(r[:len(r)-1])
}
