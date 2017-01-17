package db

import (
	"gopkg.in/yaml.v2"
	"os"
	"io/ioutil"
	"github.com/uncleandy/tcache2/log"
	"database/sql"
	"time"
	"fmt"
	_ "github.com/lib/pq"
)

type DbSettings struct {
	Host string	`yaml:"host"`
	Port int	`yaml:"port"`
	DBName string	`yaml:"dbname"`
	User string	`yaml:"user"`
	Password string	`yaml:"password"`
}

const (
	EnvDbFileConfig = "DB_CONFIG"
)

var (
	dbSettings DbSettings
	db *sql.DB
)

func Init() {
	config_file := os.Getenv(EnvDbFileConfig)
	if config_file == "" {
		log.Error.Fatalf("Db config file name required (%s environment)", EnvDbFileConfig)
	}
	_, err := os.Stat(config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Db config file '%s' not exists.", config_file)
	}

	dat, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Error.Fatalln(err)
	}

	err = yaml.Unmarshal(dat, &dbSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func Connect() *sql.DB {
	dbConnection := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		dbSettings.User,
		dbSettings.Password,
		dbSettings.Host,
		dbSettings.Port,
		dbSettings.DBName,
	)

	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		log.Error.Fatalln(err)
	}

	// Config connections.
	db.SetConnMaxLifetime(5 * time.Minute)
	//db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(50)

	return db
}

func Close() {
	if db == nil {
		return
	}

	err := db.Close()
	if err != nil {
		log.Error.Fatal(err)
	}
}

func StartTransaction() (*sql.Tx, error) {
	CheckConnect()

	return db.Begin()
}

func CommitTransaction(txn *sql.Tx) error {
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func CheckConnect() {
	if db == nil {
		db = Connect()
	} else {
		err := db.Ping()

		if err != nil {
			db = Connect()
		}
	}
}

func IsInListInt(list []int, id int) bool {
	for _, goodId := range list {
		if goodId == id {
			return true
		}
	}

	return false
}

func SendQuery(query string, params ...interface{}) (*sql.Rows, error) {
	return db.Query(query, params)
}
