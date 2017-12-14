package main

import (
	"database/sql"
	"os"
	"github.com/garyburd/redigo/redis"
	"fmt"
	_ "github.com/lib/pq"
	"regexp"
	"strings"
)

var (
	gpMasterHost = "localhost"
	gpMasterPort = "5432"
	gpDatabase   = "gpadmin"
	redisPort    = 6379
	redisConn    redis.Conn
	gpdbConn     *sql.DB
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr,
			"\nUsage: %s schema.table (will execute DDL against this table)\n", os.Args[0])
		os.Exit(1)
	}
	tableWithSchema := os.Args[1]

	gpMasterHost = getenv("GP_MASTER_HOST", gpMasterHost)
	gpMasterPort = getenv("GP_MASTER_PORT", gpMasterPort)
	gpDatabase = getenv("GP_DATABASE", gpDatabase)
	fmt.Fprintf(os.Stderr, "GP_MASTER_HOST: %s\nGP_MASTER_PORT: %s\nGP_DATABASE: %s\n",
		gpMasterHost, gpMasterPort, gpDatabase)

	// Connect to Redis
	redisConn, err := redis.DialURL(fmt.Sprintf("redis://%s:%d", gpMasterHost, redisPort))
	if err != nil {
		exitWithError(err)
	}
	defer redisConn.Close()

	// Connect to GPDB master
	connStr := fmt.Sprintf("postgres://gpadmin:password@%s:%s/%s?sslmode=disable", gpMasterHost, gpMasterPort, gpDatabase)
	gpdbConn, err = sql.Open("postgres", connStr)
	if err != nil {
		exitWithError(err)
	}
	defer gpdbConn.Close()
	err = gpdbConn.Ping()
	if err != nil {
		exitWithError(err)
	} else {
		fmt.Fprintf(os.Stderr, "Connected to GPDB (host: %s, port: %s, DB: %s)\n", gpMasterHost, gpMasterPort, gpDatabase)
	}

	/*
TODO:
Probably, need to throw the DDL into a Redis queue and have that executed independently,
via the same script which drives this periodic load process.  Just put this in with key
tableWithSchema + "-" + "DDL".

Along with that DDL to alter the heap table, this other process will need to alter the external
table corresponding to the heap table.  Adopt the convention that this table's name is the same
as the heap table, with "_kafka" appended.  Here's what that ALTER TABLE would look like:

ALTER EXTERNAL TABLE public.crimes_kafka ADD COLUMN crime_year INT, ADD COLUMN record_update_date TEXT;

*/

	// Fetch the DDL from Redis
	ddlKey := tableWithSchema + "-DDL"
	fromRedis, err := redisConn.Do("GET", ddlKey)
	if err != nil {
		exitWithError(err)
	}
	ddl := fmt.Sprintf("%s", fromRedis)

	// Execute the required "ALTER TABLE ..." commands
	execDDLOrFail(ddl)

	// Now, run the same for the corresponding external web table
	//ddlRegex := regexp.MustCompile(`ALTER +TABLE +(public\.crimes) +(.+)$`)
	ddlRegex := regexp.MustCompile(`ALTER +TABLE +(\S+) +(.+)$`)
	match := ddlRegex.FindStringSubmatch(ddl)
	extTableDdl := fmt.Sprintf("ALTER EXTERNAL TABLE %s_kafka %s", match[1], match[2])
	execDDLOrFail(extTableDdl)

	// Here, probably need to get the new ordered list of column names, join with '|', and put into Redis
	// Ref. on querying DB, fetching results: http://go-database-sql.org/retrieving.html
	var (
		colName string
		colOrder int
		colNames []string
	)
	ts := strings.Split(tableWithSchema, ".")
	sql := fmt.Sprintf(`SELECT column_name, ordinal_position
	FROM information_schema.columns
	WHERE table_schema = '%s'
	AND table_name   = '%s'
	ORDER BY 2 ASC;`, ts[0], ts[1])
	rows, err := gpdbConn.Query(sql)
	if err != nil {
		exitWithError(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&colName, &colOrder)
		if err != nil {
			exitWithError(err)
		}
		colNames = append(colNames, colName)
	}
	err = rows.Err()
	if err != nil {
		exitWithError(err)
	}
	colNamesAgg := strings.Join(colNames, "|")
	// Now, SET this in Redis
	fmt.Fprintf(os.Stderr, "Redis: SET %s \"%s\"\n", tableWithSchema, colNamesAgg)
	fromRedis, err = redisConn.Do("SET", tableWithSchema, colNamesAgg)
	if err != nil {
		exitWithError(err)
	}
	status := "SUCCEEDED"
	if fromRedis == nil {
		status = "FAILED"
	}
	fmt.Fprintf(os.Stderr, "%s\n", status)

	// Delete that DDL key in Redis
	fromRedis, err = redisConn.Do("DEL", ddlKey)
	if err != nil {
		exitWithError(err)
	}
	if fromRedis.(int64) != 1 {
		fmt.Fprintf(os.Stderr, "FAILED to delete DDL key \"%s\" from Redis\n", ddlKey)
	} else {
		fmt.Fprintf(os.Stderr, "SUCCESS deleting DDL key \"%s\" from Redis\n", ddlKey)
	}

	// That's it ...
	exitWithMessage("All done", 0)
}

func execDDLOrFail(ddl string) {
	if gpdbConn == nil {
		exitWithMessage("GPDB connection is nil\n", 1)
	}
	fmt.Fprintf(os.Stderr, "DDL: %s\n", ddl)
	_, err := gpdbConn.Exec(ddl)
	if err != nil {
		exitWithError(err)
	} else {
		fmt.Fprintf(os.Stderr, "SUCCESS\n")
	}
}

// Close any open GPDB, Redis, (other?) connections
func closeConnections() {
	if redisConn != nil {
		redisConn.Close()
		redisConn = nil
	}
	if gpdbConn != nil {
		gpdbConn.Close()
		gpdbConn = nil
	}
}

func exitWithMessage(msg string, exitCode int) {
	fmt.Fprintf(os.Stderr, "%s\n", msg)
	closeConnections()
	os.Exit(exitCode)
}

func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	closeConnections()
	os.Exit(1)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
