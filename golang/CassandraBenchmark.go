package main

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type benchConfig struct {
	seriesToInsert              int
	writeParallelStatementCount int
}

func connectSession(contactPoints []string, dataCenter string, keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(contactPoints...)

	cluster.Timeout = time.Second * 20
	cluster.Discovery.DcFilter = dataCenter
	cluster.Keyspace = keyspace

	session, err := cluster.CreateSession()

	if err != nil {
		fmt.Println("Can't open session")
		return nil, err
	}

	fmt.Println("Connected")

	return session, nil
}

func cleanup(session *gocql.Session) error {
	if err := session.Query("TRUNCATE \"SerieId\";").Exec(); err != nil {
		return err
	}

	if err := session.Query("TRUNCATE \"Timeserie\"").Exec(); err != nil {
		return err
	}

	return nil
}

func main() {
	// Connect to Cassandra cluster
	session, err := connectSession([]string{"<INSERT_YOUR_CONTACT_HOST_HERE>"}, "<INSERT_YOUR_DC_HERE>", "CsharpDriverBenchmark")
	defer session.Close()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Cleanup test environment
	fmt.Println("Cleaning up test environment")
	err = cleanup(session)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Insert test data
	fmt.Println("Starting write test")
	startTimestamp := time.Now()
	insertedCount, err := insert(session, benchConfig{
		seriesToInsert:              5000,
		writeParallelStatementCount: 500,
	})

	timeSpent := time.Now().Sub(startTimestamp)
	fmt.Println("Insertion complete, ", insertedCount, " data points in ", timeSpent, " (", int(float64(insertedCount)/timeSpent.Seconds()), " point/s)")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

}
