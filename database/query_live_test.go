package database

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const ( // Testing defaults
	defaultPostgresPwd  = "secret"
	defaultPostgresSSL  = "sslmode=disable"
	defaultPostgresUser = "postgres"
	defaultPostgresHost = "postgres"
	defaultPostgresPort = "5432"
	defaultDatabaseName = "local"
)

type Parent struct {
	ParentId int       `column:"parent_id" pk:"true"`
	Float    float32   `column:"float"`
	Data     string    `column:"data"`
	Dttm     time.Time `column:"dttm"`
}

type Child struct {
	ParentId int       `column:"parent_id" fk:"true"`
	ChildId  int       `column:"child_id"  pk:"generated"`
	Data     string    `column:"data"`
	Dttm     time.Time `column:"dttm"`
}

type ChildNoKey struct {
	ParentId int
	Data     string
	Dttm     time.Time
}

var (
	db             *sql.DB
	DatabaseSchema string
)

var ( // Test scripts
	scriptsBefore = []string{
		`DROP SCHEMA IF EXISTS "%s" CASCADE`,
		`CREATE SCHEMA "%s"`,
		`CREATE TABLE "%s".parent (parent_id integer, float numeric(10,2), data Text, dttm timestamp with time zone, PRIMARY KEY(parent_id))`,
		`CREATE TABLE "%s".child (parent_id integer, child_id SERIAL, data Text, dttm timestamp, PRIMARY KEY(parent_id, child_id))`,
	}
	scriptsAfter = []string{
		`DROP SCHEMA IF EXISTS "%s" CASCADE`,
	}
)

var regex = regexp.MustCompile("[^a-zA-Z0-9-]+")

func connect(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	postgresSSL := envWithDefault("POSTGRES_SSL", defaultPostgresSSL)
	postgresPwd := envWithDefault("POSTGRES_PASSWORD", defaultPostgresPwd)
	postgresUser := envWithDefault("POSTGRES_USER", defaultPostgresUser)
	postgresHost := envWithDefault("POSTGRES_HOST", defaultPostgresHost)
	postgresPort := envWithDefault("POSTGRES_PORT", defaultPostgresPort)
	databaseName := envWithDefault("DATABASE_NAME", defaultDatabaseName)
	databaseDriver := os.Getenv("DATABASE_DRIVER")
	DatabaseSchema = os.Getenv("DATABASE_SCHEMA")

	if databaseDriver == "" {
		t.Skip("database driver (EnvVar:DATABASE_DRIVER) not defined")
	}

	if DatabaseSchema == "" {
		text, err := uuid.NewRandom()
		if err != nil {
			t.Fatalf("Unable to create uuid: %v", err)
		}
		DatabaseSchema = regex.ReplaceAllString(base64.StdEncoding.EncodeToString([]byte(text.String())), "")
	}

	var psqlInfo string
	if databaseURL == "" {
		psqlInfo = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s %s connect_timeout=30",
			postgresHost, postgresPort, postgresUser, postgresPwd, databaseName, postgresSSL)
	} else {
		psqlInfo = databaseURL
	}

	var err error
	if db, err = sql.Open(databaseDriver, psqlInfo); err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}

	var tries int
	for tries, err = 0, db.Ping(); tries < 30 && err != nil; tries, err = tries+1, db.Ping() {
		time.Sleep(500 * time.Millisecond)
		err = db.Ping()
	}
	if err != nil {
		t.Fatalf("Error verifying connection to the database: %v", err)
	}

	if _, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", DatabaseSchema)); err != nil {
		t.Fatalf("error setting postgres search_path: %v", err)
	}
}

func InitDB(t *testing.T) {
	connect(t)
	executeScripts(t, scriptsBefore)
}

func Disconnect(t *testing.T) {
	executeScripts(t, scriptsAfter)
	if db != nil {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close connection: %v", err)
			t.FailNow()
		}
	}
}

func executeScripts(t *testing.T, scripts []string) {
	for _, stmt := range scripts {
		if strings.Contains(stmt, "%s") {
			stmt = fmt.Sprintf(stmt, DatabaseSchema)
		}
		if _, err := db.Exec(stmt); err != nil {
			t.Errorf("%s\nfailed to execute script: %v", stmt, err)
			t.FailNow()
		}
	}
}

func envWithDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
