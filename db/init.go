package db

import (
	"gopkg.in/yaml.v2"
	"os"
	"io/ioutil"
	"github.com/uncleandy/tcache2/log"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"regexp"
	"errors"
)

type DbSettings struct {
	Host string	`yaml:"host"`
	Port int	`yaml:"port"`
	DBName string	`yaml:"dbname"`
	User string	`yaml:"user"`
	Password string	`yaml:"password"`
}

type DbConnection struct {
	Settings *DbSettings
	Db *sql.DB
	Transaction *sql.Tx
}

const (
	EnvDbFileConfig = "DB_CONFIG"
)

var (
	CurrentDbSettings *DbSettings
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

	err = yaml.Unmarshal(dat, &CurrentDbSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}
}

func (conn *DbConnection) Init(db_settings *DbSettings) {
	conn.Settings = db_settings
}

func Connect() *sql.DB {
	return ConnectBy(CurrentDbSettings)
}

func ConnectBy(settings *DbSettings) *sql.DB {
	dbConnection := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		settings.User,
		settings.Password,
		settings.Host,
		settings.Port,
		settings.DBName,
	)

	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		log.Error.Fatalln(err)
	}

	// Config connections.
	db.SetConnMaxLifetime(0)
	//db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(50)

	return db
}

func (conn *DbConnection) Connect() {
	conn.Db = ConnectBy(conn.Settings)
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


func (conn *DbConnection) Close() {
	if conn.Db == nil {
		return
	}

	err := conn.Db.Close()
	if err != nil {
		log.Error.Fatal(err)
	}
}

func StartTransaction() (*sql.Tx, error) {
	CheckConnect()

	return db.Begin()
}

func (conn *DbConnection) StartTransaction() error {
	if conn.Transaction != nil {
		return errors.New("START TRANSACTION: Transaction already started.")
	}

	conn.CheckConnect()

	trx, err := conn.Db.Begin()
	if err == nil {
		conn.Transaction = trx
	} else {
		conn.Transaction = nil
	}

	return err
}

func CommitTransaction(txn *sql.Tx) error {
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func (conn *DbConnection) CommitTransaction() error {
	if conn.Transaction == nil {
		return errors.New("COMMIT TRANSACTION: Transaction not started.")
	}

	err := conn.Transaction.Commit()
	if err != nil {
		return err
	}

	conn.Transaction = nil
	return nil
}

func CheckConnect() {
	db = CheckConnectBy(db, CurrentDbSettings)
}

func CheckConnectBy(checked_db *sql.DB, db_settings *DbSettings) *sql.DB {
	if checked_db == nil {
		return ConnectBy(db_settings)
	} else {
		err := checked_db.Ping()

		if err != nil {
			log.Error.Println("DB ping error: ", err)
			return ConnectBy(db_settings)
		}
	}

	return checked_db
}

func (conn *DbConnection) CheckConnect() {
	conn.Db = CheckConnectBy(conn.Db, conn.Settings)
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
	return db.Query(query, params...)
}

func (conn *DbConnection) SendQuery(query string, params ...interface{}) (*sql.Rows, error) {
	return conn.Db.Query(query, params...)
}

func SendQueryParamsTrx(txn *sql.Tx, query string, params ...interface{}) error {
	stmt, err := txn.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(params...)
	if err != nil {
		return err
	}

	if err = stmt.Close(); err != nil {
		return err
	}

	return nil
}

func (conn *DbConnection) SendQueryParamsTrx(query string, params ...interface{}) error {
	if conn.Transaction == nil {
		return errors.New("SEND QUERY: Transaction not started.")
	}

	return SendQueryParamsTrx(conn.Transaction, query, params...)
}

func EscapedBy(source string, symbol string, code string) string {
	return strings.Replace(source, symbol, code, -1)
}

func Escaped(source string) string {
	return EscapedBy(source, "'", "''")
}

// Convert time:
// 2017-01-11T00:00:00Z -> 2017-01-11
// 2017-01-11T00:00:01Z -> 2017-01-11 00:00:01
func ConvertTime(src string) string {
	split := strings.Split(src, "T")
	if len(split) < 2 {
		split_s := regexp.MustCompile("[^\\d\\:\\-]+")
		split = split_s.Split(src, -1)
		if len(split) <= 1 {
			return src
		}
	}

	if split[1] == "00:00:00Z" || split[1] == "00:00:00" {
		return split[0]
	} else {
		time := split[1]
		time = strings.Replace(time, "Z", "", -1)
		return split[0] + " " + time
	}

	return src
}
