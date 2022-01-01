package main

import (
	"database/sql"
	"fmt"
	"log"
	"sandbox/database/database"
	"time"

	_ "github.com/lib/pq"
)

type Parent struct {
	ParentId int       `column:"parent_id" pk:"true"`
	Data     string    `column:"data"`
	Dttm     time.Time `column:"dttm"`
	Count    int       `column:"count"`
}

type Child struct {
	ParentId int  `column:"parent_id" pk:"true" fk:"true"`
	ChildId  int  `column:"child_id"  pk:"true"`
	Enabled  bool `column:"enable"`
	Data     int  `column:"data"`
}

func main() {
	db, err := Connect()
	if err != nil {
		log.Fatalf("Connection error: %v", err.Error())
	}

	fmt.Sprintf("%v", db)
	pt, err := database.NewTable("schema", "parent", &Parent{})
	if err != nil {
		log.Fatalf("Failed to map parent table: %v", err.Error())
	}

	//ct, err := database.NewTable("schema", "child", &Child{})
	//if err != nil {
	//	log.Fatalf("Failed to map parent table: %v", err.Error())
	//}
	//
	parent := Parent{
		ParentId: 1,
		Data:     "Test",
		Count:    99,
		Dttm:     time.Now(),
	}

	pt.Insert(db, &parent)

}

func Connect() (*sql.DB, error) {
	var db *sql.DB
	var err error

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s %s connect_timeout=30",
		"127.0.0.1", 5432, "postgres", "Teradata2021!", "local", "sslmode=disable")

	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres: %v", err)
	}
	for i := 0; i < 60; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("error verifying connection to postgres: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", "schema"))
	if err != nil {
		return nil, fmt.Errorf("error setting postgres search_path: %v", err)
	}

	log.Println("Successfully connected to Postgres!")

	return db, nil
}
