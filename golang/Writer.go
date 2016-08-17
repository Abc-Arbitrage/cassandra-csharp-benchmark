package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

var ttl = int(time.Duration(time.Hour * 24 * 8).Seconds())

func generateSerieIds(count int) []gocql.UUID {
	result := make([]gocql.UUID, count)

	for i := 0; i < count; i++ {
		result[i], _ = gocql.RandomUUID()
	}

	return result
}

func insertSerieIds(session *gocql.Session, serieIdsToInsert []gocql.UUID) error {
	insertSerieQuery := session.Query("INSERT INTO \"SerieId\" (\"SerieId\") VALUES (?);")

	for _, id := range serieIdsToInsert {
		if err := insertSerieQuery.Bind(id).Exec(); err != nil {
			return err
		}
	}
	return nil
}

type dataToInsert struct {
	serieID  gocql.UUID
	dateTime time.Time
	value    float64
}

func insert(session *gocql.Session, config benchConfig) (int64, error) {
	pointsPerSerie := 18000 // Average number of points per day taken from real data

	serieIdsToInsert := generateSerieIds(config.seriesToInsert)

	if err := insertSerieIds(session, serieIdsToInsert); err != nil {
		return 0, err
	}

	pendingInsertsChannel := make(chan dataToInsert, 100000)
	var allWorkersAreDoneWaitGroup sync.WaitGroup

	// Data generation routine
	go func() {
		random := rand.New(rand.NewSource(123))
		timestamp := time.Now().UTC().Truncate(time.Hour * 24)

		for i := 0; i < pointsPerSerie; i++ {
			timestamp = timestamp.Add(time.Second)
			for _, serieID := range serieIdsToInsert {
				pendingInsertsChannel <- dataToInsert{dateTime: timestamp, serieID: serieID, value: random.Float64()}
			}
		}
		close(pendingInsertsChannel)
	}()

	var insertedRowCount int64

	// Data insertion routines
	for i := 0; i < config.writeParallelStatementCount; i++ {
		allWorkersAreDoneWaitGroup.Add(1)
		go func() {
			defer allWorkersAreDoneWaitGroup.Done()
			query := session.Query("INSERT INTO \"Timeserie\" (\"SerieId\", \"Day\", \"UtcDate\", \"Value\") VALUES (?, ?, ?, ?) USING TTL ?;")
			for dataToInsert := range pendingInsertsChannel {
				if err := query.Bind(dataToInsert.serieID, dataToInsert.dateTime.Truncate(time.Hour*24), dataToInsert.dateTime, dataToInsert.value, ttl).Exec(); err != nil {
					fmt.Println(err.Error())
				}
				atomic.AddInt64(&insertedRowCount, 1)
			}
		}()
	}
	allWorkersAreDoneWaitGroup.Wait()
	return insertedRowCount, nil
}
