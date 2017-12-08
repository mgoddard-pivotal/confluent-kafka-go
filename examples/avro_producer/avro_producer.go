// Example function-based Apache Kafka producer
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
	"fmt"
	"os"
	"io/ioutil"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

/*
 * Alter this to accept a list of file names, in addition to the broker and topic arguments.
 * For each filename listed:
 *   (a) Open the file
 * 	 (b) Read the entire file
 *   (c) Write the contents into a []byte
 *   (d) Publish this []byte to the topic
 *   (e) Close the input file.
 */

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic> <file.avro> [<file2.avro> ...]\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)
	var avroFile string
	for  i := 3; i < len(os.Args); i++ {
		avroFile = os.Args[i]
		fmt.Fprintf(os.Stderr, "Reading Avro file %s now ...\n", avroFile)
		value, err := ioutil.ReadFile(avroFile)
		if err != nil {
			log.Fatal(err)
		}
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: value}, deliveryChan)
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			fmt.Fprintf(os.Stderr, "Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			/*
			   fmt.Fprintf(os.Stderr, "Delivered message to topic %s [%d] at offset %v\n",
			     *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			*/
		}
	}
	close(deliveryChan)
}
