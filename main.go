package main

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/jinzhu/configor"
)

var schema = "CREATE TABLE `nfdump` (" +
	"`id` int(11) NOT NULL AUTO_INCREMENT," +
	"`datetime` datetime NOT NULL," +
	"`path` varchar(45) NOT NULL," +
	"`group` varchar(255) NOT NULL," +
	"`dev` varchar(255) NOT NULL," +
	"`year` int(4) NOT NULL," +
	"`month` int(2) NOT NULL," +
	"`day` int(2) NOT NULL," +
	"`file` varchar(100) NOT NULL," +
	"PRIMARY KEY (`id`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

// Config : structure config file
var Config = struct {
	Minute string `default:"5"`

	DB struct {
			Name     string `required:"true"`
			User     string `default:"root"`
			Password string `required:"true" env:"DBPassword"`
			Port     string `default:"3306"`
		}
}{}

// Table : this structure for create or truncate table
type Table struct {
	Table string `db:"table_name"`
}

func main() {

	configor.Load(&Config, "config.yml")

	go process() // this for first start
	tickTime, _ := strconv.Atoi(Config.Minute)
	for range time.Tick(time.Minute * time.Duration(tickTime)) {
		go process()
	}
}

func process() {

	fmt.Println(Config)
	conn, err := sqlx.Connect("mysql", Config.DB.User +":"+ Config.DB.Password +"@tcp(localhost:"+ Config.DB.Port +")/"+ Config.DB.Name)
	if err != nil {
		panic(err)
	}

	var res Table
	err = conn.Get(&res, "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '"+ Config.DB.Name +"' AND table_name = 'nfdump'")
	if err != nil {

		log.Println("Create tamble nfdump")
		conn.MustExec(schema)

	} else if res.Table == "nfdump" {

		conn.MustExec("truncate table nfdump")

	}

	layout := "200601021504"
	hash := make(map[time.Time][]string)
	path := "/data/nfsen/profiles-data/*/*/*/*/*/*"

	files, err := filepath.Glob(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {

		// fmt.Println(file)
		strs := strings.Split(file, ".")
		folders := strings.Split(file, "/")
		//fmt.Println(folders)

		for i, str := range strs {
			// If key 1, formatting the date time
			if i == 1 {

				t, err := time.Parse(layout, str)
				if err != nil {
					fmt.Println(err)
				}

				hash[t] = folders
			}
		}
	}

	for k, v := range hash {

		_, err := conn.Exec("INSERT INTO nfdump ("+
			"`datetime`, `path`, `group`, `dev`, `year`, `month`, `day`, `file`)"+
			"VALUES(?,?,?,?,?,?,?,?)",
			k.Format("2006-01-02 15:04:05"), v[0]+"/"+v[1]+"/"+v[2]+"/"+v[3]+"/", v[4], v[5], v[6], v[7], v[8], v[9])
		if err != nil {
			panic(err)
		}
	}

	conn.Close()
}
