package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gopkg.in/gomail.v2"
	"gopkg.in/ini.v1"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strconv"
	"strings"
)

func init() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.DisableQuote = true
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	logrus.SetFormatter(customFormatter)
	logrus.SetOutput(&lumberjack.Logger{
		Filename: "partition.log",
		MaxSize:  500, //M
		MaxAge:   30,  //days
	})

}

func sendMail(body string){
	cfgserver, err := ini.Load("config.ini")
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	host := cfgserver.Section("mail").Key("host").Value()
	username := cfgserver.Section("mail").Key("username").Value()
	password := cfgserver.Section("mail").Key("password").Value()
	recipients := cfgserver.Section("mail").Key("recipients").Value()
	subject := cfgserver.Section("mail").Key("subject").Value()
	m := gomail.NewMessage()
	m.SetHeader("From",username)
	recvArr := strings.Split(recipients,",")
	addresses := make([]string, len(recvArr))
	for i, recipient := range recvArr {
		addresses[i] = m.FormatAddress(recipient, "")
	}
	m.SetHeader("To", addresses...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)
	d := gomail.NewDialer(host, 465, username, password)
	err2 := d.DialAndSend(m)
	if err2 != nil{
		logrus.Error(err2)
	}
	logrus.Info("邮件发送成功")
}

func GetDB(url string) *sql.DB {
	db, err := sql.Open("mysql", url)
	if err != nil {
		panic(err)
	}
	return db
}

func GetSelectStmt(db *sql.DB) *sql.Stmt {
	sqlstr := "select"+
		" p.TABLE_SCHEMA,"+
		" p.TABLE_NAME,"+
		" p.PARTITION_NAME,"+
		" p.PARTITION_DESCRIPTION"+
		" from"+
		" information_schema.PARTITIONS p"+
		" where"+
		" p.TABLE_SCHEMA = ?"+
		" and p.TABLE_NAME = ?"+
		" order by p.PARTITION_ORDINAL_POSITION"
	stmt, _ := db.Prepare(sqlstr)
	return stmt
}

func DropPartition(db *sql.DB,table_name string,partition_name string){
	sqlstr := "alter table "+table_name +" drop partition "+partition_name
	_,err := db.Exec(sqlstr)
	if err != nil {
		fmt.Println("删除分区失败", err)
		os.Exit(1)
	}
}

func getDays(db *sql.DB) int64{
	sqlstr := "SELECT to_days(date_sub(date_format(now(),'%Y%m%d'), INTERVAL 3 MONTH)) as days"
	rows,_ := db.Query(sqlstr)
	var days int64
	rows.Next()
	rows.Scan(&days)
	return days
}

func main() {

	cfgserver, err := ini.Load("config.ini")
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	url := cfgserver.Section("server").Key("url").Value()
	table_schema := cfgserver.Section("table").Key("table_schema").Value()
	table_names := cfgserver.Section("table").Key("table_names").Value()
	tableArr := strings.Split(table_names,",")
	db := GetDB(url)
	stmtSelect := GetSelectStmt(db)
	days := getDays(db)
	var sb strings.Builder
	for _, table_name := range tableArr {
		rows,_ := stmtSelect.Query(table_schema,table_name)
		for rows.Next(){
			var table_schema, table_name, partition_name, partition_description string
			if err := rows.Scan(&table_schema,&table_name, &partition_name, &partition_description); err != nil {
				logrus.Error(err)
			}
			pd,_ := strconv.ParseInt(partition_description,10,64)
			if pd == 0 {
				continue
			}
			if pd < days {
				DropPartition(db,table_name,partition_name)
				sb.WriteString("删除分区(库:"+table_schema+",表:"+table_name+",分区:"+partition_name+")<br/>")
				fmt.Printf("删除(库:%s,表:%s,分区:%s)\n",table_schema,table_name,partition_name)
				logrus.Infof("删除分区(库:%s,表:%s,分区:%s)",table_schema,table_name,partition_name)
			}
		}
	}

	if len(sb.String()) !=0 {
		sendMail(sb.String())
	}

}

