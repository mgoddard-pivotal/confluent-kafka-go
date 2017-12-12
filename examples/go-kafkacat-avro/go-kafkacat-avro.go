// Example kafkacat clone written in Golang
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/garyburd/redigo/redis"
	_ "github.com/lib/pq"
	"github.com/linkedin/goavro"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	verbosity    = 1
	exitEOF      = false
	eofCnt       = 0
	partitionCnt = 0
	keyDelim     = ""
	sigs         chan os.Signal
	isAvro       = false
	gpXid        = ""
	gpSegmentId  = ""
	gpMasterHost = ""
	gpMasterPort = ""
	gpDatabase   = ""
	outputDelim  = ","
	redisPort    = 6379
	redisConn    redis.Conn
	gpdbConn     *sql.DB
)

var avroToSqlType = map[string]string{
	"boolean": "BOOL",
	"int":     "INT",
	"long":    "BIGINT",
	"float":   "FLOAT4",
	"double":  "FLOAT8",
	"bytes":   "BYTEA",
	"string":  "TEXT",
}

// These need to be accessible globally
var colNames []string
var colNameToType map[string]string

const redisLockLifetimeMS int = 24 * 60 * 60 * 1000 // This is the lifetime of the Redis mutex for a DDL operation, in ms

func runProducer(config *kafka.ConfigMap, topic string, partition int32) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		exitWithMessage(fmt.Sprintf("Failed to create producer: %s\n", err), 1)
	}

	fmt.Fprintf(os.Stderr, "Created Producer %v, topic %s [%d]\n", p, topic, partition)

	tp := kafka.TopicPartition{Topic: &topic, Partition: partition}

	go func(drs chan kafka.Event) {
		for ev := range drs {
			m, ok := ev.(*kafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				fmt.Fprintf(os.Stderr, "%% Delivery error: %v\n", m.TopicPartition)
			} else if verbosity >= 2 {
				fmt.Fprintf(os.Stderr, "%% Delivered %v\n", m)
			}
		}
	}(p.Events())

	reader := bufio.NewReader(os.Stdin)
	stdinChan := make(chan string)

	go func() {
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			line = strings.TrimSuffix(line, "\n")
			if len(line) == 0 {
				continue
			}

			stdinChan <- line
		}
		close(stdinChan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdinChan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{TopicPartition: tp}

			if keyDelim != "" {
				vec := strings.SplitN(line, keyDelim, 2)
				if len(vec[0]) > 0 {
					msg.Key = ([]byte)(vec[0])
				}
				if len(vec) == 2 && len(vec[1]) > 0 {
					msg.Value = ([]byte)(vec[1])
				}
			} else {
				msg.Value = ([]byte)(line)
			}

			p.ProduceChannel() <- &msg
		}
	}

	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	p.Flush(10000)
	fmt.Fprintf(os.Stderr, "%% Closing\n")
	p.Close()
}

// TODO: Modify this for Avro
func runConsumer(config *kafka.ConfigMap, topics []string) {

	c, err := kafka.NewConsumer(config)
	if err != nil {
		exitWithMessage(fmt.Sprintf("Failed to create consumer: %s\n", err), 1)
	}

	fmt.Fprintf(os.Stderr, "%% Created Consumer %v\n", c)

	c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {

		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "AssignedPartitions %v\n", e)
				c.Assign(e.Partitions)
				partitionCnt = len(e.Partitions)
				eofCnt = 0
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "RevokedPartitions %v\n", e)
				c.Unassign()
				partitionCnt = 0
				eofCnt = 0
			case *kafka.Message:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "Message %v:\n", e.TopicPartition)
				}
				if keyDelim != "" {
					if e.Key != nil {
						fmt.Printf("%s%s", string(e.Key), keyDelim)
					} else {
						fmt.Printf("%s", keyDelim)
					}
				}
				if isAvro {
					/*
						TODO: What is the best way to handle the case where some data has been pulled out of Kafka, but
						another process is making DDL changes?  Need to ensure that the offsets are updated appropriately,
						but that this current Kafka message is available the next time this program runs.
					*/
					if redisLockExists() {
						exitWithMessage("Lock exists in Redis -- quitting", 0)
					}
					// Get access to the Avro schema
					ior := bytes.NewReader(e.Value)
					ocf, err := goavro.NewOCFReader(ior)
					if err != nil {
						exitWithError(err)
					}
					codec := ocf.Codec()
					var schemaStr string
					schemaStr = codec.Schema()
					fmt.Fprintf(os.Stderr, "Schema: %s\n", schemaStr)
					var schema map[string]interface{}
					if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
						exitWithError(err)
					}
					// The "namespace" field contains the table name
					tableName := schema["namespace"].(string)
					fmt.Fprintf(os.Stderr, "Table name: %s\n", tableName)
					var fromRedis interface{}
					fromRedis, err = redisConn.Do("GET", tableName)
					if err != nil {
						exitWithError(err)
					}
					colNamesAggRedis := fmt.Sprintf("%s", fromRedis)
					// The "doc" field is assumed to contain a pipe-separated list of column names
					colNamesAgg := schema["doc"].(string)
					fmt.Fprintf(os.Stderr, "colNames (schema): %s\ncolNames (Redis): %s\n", colNamesAgg, colNamesAggRedis)
					colNamesAvro := strings.Split(colNamesAgg, "|")
					colNameToType = make(map[string]string)
					fmt.Fprintf(os.Stderr, "colNames: %v\n", colNamesAvro)
					colsWithTypes := schema["fields"].([]interface{})
					for _, val := range colsWithTypes {
						colMeta := val.(map[string]interface{})
						colName := colMeta["name"]
						colNames = append(colNames, colName.(string))
						colTypeTmp := colMeta["type"]
						var colType string
						// This colTypeTmp could be either string or array of string
						switch t := colTypeTmp.(type) {
						case string:
							colType = colTypeTmp.(string)
						case []interface{}:
							s := make([]string, len(t))
							for i, v := range t {
								s[i] = fmt.Sprint(v)
							}
							colType = s[0]
						default:
							fmt.Fprintf(os.Stderr, "colType UNKNOWN: %v\n", colTypeTmp)
						}
						colNameToType[colName.(string)] = colType
					}
					//fmt.Fprintf(os.Stderr, "colNameToType: %v, colNames: %v\n", colNameToType, colNames)
					if strings.HasPrefix(colNamesAggRedis, colNamesAgg) {
						fmt.Fprintf(os.Stderr, "Schema is consistent\n")
					} else {
						fmt.Fprintf(os.Stderr, "Schema must be updated\n")
						// Set a lock in Redis
						fromRedis, err = redisConn.Do("SET", gpXid, gpSegmentId, "NX", "PX", redisLockLifetimeMS)
						if err != nil {
							exitWithError(err)
						}
						if fromRedis == nil {
							fmt.Fprintf(os.Stderr, "FAILED to get lock -- quitting\n")
						}
						// Determine which columns need to be added, with their types
						alterTable := ""
						colNamesExisting := strings.Split(colNamesAggRedis, "|")
						for i := len(colNamesExisting); i < len(colNamesAvro); i++ {
							newColName := colNamesAvro[i]
							newColAvroType := colNameToType[newColName]
							newColSqlType := avroToSqlType[newColAvroType]
							//fmt.Fprintf(os.Stderr, "Add column \"%s\"\n", newColName)
							if len(alterTable) > 0 {
								alterTable += ", "
							}
							alterTable += "ADD COLUMN " + newColName + " " + newColSqlType
						}
						alterTable = "ALTER TABLE " + tableName + " " + alterTable
						fmt.Fprintf(os.Stderr, "DDL: %s\n", alterTable)

						// Execute the required "ALTER TABLE ..." commands
						_, err = gpdbConn.Exec(alterTable)
						if err != nil {
							exitWithError(err)
						} else {
							fmt.Fprintf(os.Stderr, "SUCCESSFULLY ran that DDL\n")
						}

						// Update Redis with the new colNamesAgg value
						fromRedis, err = redisConn.Do("SET", tableName, colNamesAgg)
						if err != nil {
							exitWithError(err)
						}
						if fromRedis == nil {
							fmt.Fprintf(os.Stderr, "FAILED to update column names for table \"%s\"\n", tableName)
						} else {
							fmt.Fprintf(os.Stderr, "SUCCEEDED in updating column names for table \"%s\"\n", tableName)
						}
						// FIXME: exiting here will not update the Kafka topic's offset for already consumed data.
						exitWithMessage("Exiting after the DDL operation", 0)
					}
					avroToCsv(ocf) // This prints the CSV version
					fmt.Fprintf(os.Stderr, "Wrote Avro message\n")
				} else {
					fmt.Println(string(e.Value))
				}
			case kafka.PartitionEOF:
				fmt.Fprintf(os.Stderr, "%% Reached %v\n", e)
				eofCnt++
				if exitEOF && eofCnt >= partitionCnt {
					run = false
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			case kafka.OffsetsCommitted:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
				}
			default:
				fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "%% Closing consumer\n")
	c.Close()
}

func avroToCsv(ocf *goavro.OCFReader) {
	//fmt.Fprintf(os.Stderr, "In avroToCsv\n")
	codec := ocf.Codec()
	for ocf.Scan() {
		//fmt.Fprintf(os.Stderr, "In avroToCsv -> ocf.Scan\n")
		datum, err := ocf.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: avroToCsv() %s\n", err)
			continue
		}
		buf, err := codec.TextualFromNative(nil, datum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: avroToCsv() %s\n", err)
			continue
		}
		// HERE: buf contains a single line of JSON, a single JSON document
		// NOTE now the "id" field differs (it's not nullable)
		//
		// {"description":{"string":"SIMPLE"},"domestic":{"boolean":false},"x_coord":{"float":1.1542e+06},"id":10035257, ... }
		//
		// Delimiter: outputDelim (",")
		// Detect whether a field contains a delimiter, so needs to be quoted: strings.Contains(jsonValue, outputDelim)
		jsonMap := make(map[string]string)
		var f interface{}
		//err = json.Unmarshal(buf, &f)
		d := json.NewDecoder(strings.NewReader(string(buf)))
		d.UseNumber()
		err = d.Decode(&f)
		if err != nil {
			exitWithError(err)
		}
		m := f.(map[string]interface{})
		for k, v := range m {
			switch vv := v.(type) {
			case string, float64, bool, int:
				jsonMap[k] = fmt.Sprint(vv)
			case map[string]interface{}:
				for _, val := range vv {
					jsonMap[k] = fmt.Sprint(val)
				}
			default:
				// This would be a non-null field
				jsonMap[k] = fmt.Sprint(vv)
			}
		}
		colVals := make([]string, len(colNames))
		for i, v := range colNames {
			val, ok := jsonMap[v]
			if ok {
				if strings.Contains(val, outputDelim) {
					colVals[i] = "\"" + val + "\""
				} else {
					colVals[i] = val
				}
			} else {
				colVals[i] = ""
			}
		}
		//fmt.Println(string(buf))
		fmt.Println(strings.Join(colVals, outputDelim))
	}
}

// Return true if there's a lock; false if not
func redisLockExists() bool {
	fromRedis, err := redisConn.Do("GET", gpXid)
	if err != nil {
		exitWithError(err)
	}
	return fromRedis != nil
}

type configArgs struct {
	conf kafka.ConfigMap
}

func (c *configArgs) String() string {
	return "FIXME"
}

func (c *configArgs) Set(value string) error {
	return c.conf.Set(value)
}

func (c *configArgs) IsCumulative() bool {
	return true
}

// TODO: Replace kingpin with flag (https://gobyexample.com/command-line-flags)
func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	_, libver := kafka.LibraryVersion()
	kingpin.Version(fmt.Sprintf("confluent-kafka-go (librdkafka v%s)", libver))

	// Default config
	var confargs configArgs
	confargs.conf = kafka.ConfigMap{"session.timeout.ms": 6000}

	/* General options */
	brokers := kingpin.Flag("broker", "Bootstrap broker(s)").Required().String()
	kingpin.Flag("config", "Configuration property (prop=val)").Short('X').PlaceHolder("PROP=VAL").SetValue(&confargs)
	keyDelimArg := kingpin.Flag("key-delim", "Key and value delimiter (empty string=dont print/parse key)").Default("").String()
	verbosityArg := kingpin.Flag("verbosity", "Output verbosity level").Short('v').Default("1").Int()

	/* Producer mode options */
	modeP := kingpin.Command("produce", "Produce messages")
	topic := modeP.Flag("topic", "Topic to produce to").Required().String()
	partition := modeP.Flag("partition", "Partition to produce to").Default("-1").Int()

	/* Consumer mode options */
	modeC := kingpin.Command("consume", "Consume messages").Default()
	group := modeC.Flag("group", "Consumer group").Required().String()
	topics := modeC.Arg("topic", "Topic(s) to subscribe to").Required().Strings()
	initialOffset := modeC.Flag("offset", "Initial offset").Short('o').Default(kafka.OffsetBeginning.String()).String()
	exitEOFArg := modeC.Flag("eof", "Exit when EOF is reached for all partitions").Bool()
	avroArg := modeC.Flag("avro", "Assume data is in Avro (binary) format").Default("false").Bool()

	mode := kingpin.Parse()

	verbosity = *verbosityArg
	keyDelim = *keyDelimArg
	exitEOF = *exitEOFArg
	isAvro = *avroArg
	confargs.conf["bootstrap.servers"] = *brokers

	// All these are present within the external web table environment of GPDB segment hosts
	gpXid = os.Getenv("GP_XID")
	gpSegmentId = os.Getenv("GP_SEGMENT_ID")
	gpMasterHost = os.Getenv("GP_MASTER_HOST")
	gpMasterPort = os.Getenv("GP_MASTER_PORT")
	gpDatabase = os.Getenv("GP_DATABASE")
	fmt.Fprintf(os.Stderr, "GP_XID: %s\nGP_SEGMENT_ID: %s\n", gpXid, gpSegmentId)

	if isAvro {
		var err error
		// Connect to Redis
		if redisConn == nil {
			redisConn, err = redis.DialURL(fmt.Sprintf("redis://%s:%d", os.Getenv("GP_MASTER_HOST"), redisPort))
			if err != nil {
				exitWithError(err)
			}
			defer redisConn.Close()
		}
		// Quit immediately if some peer process is updating the DDL for the table
		if redisLockExists() {
			exitWithMessage("Exiting due to a Redis lock for this GP_XID (another process is executing DDL)", 0)
		}
		// Connect to GPDB master
		// Ref:
		// https://godoc.org/github.com/lib/pq
		// http://go-database-sql.org/accessing.html
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
	}

	switch mode {
	case "produce":
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"produce.offset.report": true}
		runProducer((*kafka.ConfigMap)(&confargs.conf), *topic, int32(*partition))

	case "consume":
		confargs.conf["group.id"] = *group
		confargs.conf["go.events.channel.enable"] = true
		confargs.conf["go.application.rebalance.enable"] = true
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"auto.offset.reset": *initialOffset}
		runConsumer((*kafka.ConfigMap)(&confargs.conf), *topics)
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
