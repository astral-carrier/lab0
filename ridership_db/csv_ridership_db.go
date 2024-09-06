package ridershipDB

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

type CsvRidershipDB struct {
	idIdxMap      map[string]int
	csvFile       *os.File
	csvReader     *csv.Reader
	num_intervals int
}

func (c *CsvRidershipDB) Open(filePath string) error {
	c.num_intervals = 9

	// Create a map that maps MBTA's time period ids to indexes in the slice
	c.idIdxMap = make(map[string]int)
	for i := 1; i <= c.num_intervals; i++ {
		timePeriodID := fmt.Sprintf("time_period_%02d", i)
		c.idIdxMap[timePeriodID] = i - 1
	}

	// create csv reader
	csvFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	c.csvFile = csvFile
	c.csvReader = csv.NewReader(c.csvFile)

	return nil
}

// TODO: some code goes here
// Implement the remaining RidershipDB methods

func (c *CsvRidershipDB) GetRidership(lineId string) ([]int64, error) {
	periodsToRidership := make(map[string]int64)

	for {
		row, readError := c.csvReader.Read()

		if readError == io.EOF {
			break
		} else if readError != nil {
			return nil, readError
		}

		if row[0] == lineId {
			rowRidership, conversionError := strconv.Atoi(row[4])

			if conversionError != nil {
				return nil, conversionError
			}

			periodsToRidership[row[2]] += int64(rowRidership)
		}
	}

	ridershipArray := make([]int64, 0)

	for i := 1; i <= c.num_intervals; i++ {
		timePeriodID := fmt.Sprintf("time_period_%02d", i)

		ridershipArray = append(ridershipArray, periodsToRidership[timePeriodID])
	}

	return ridershipArray, nil
}

func (c *CsvRidershipDB) Close() error {
	return c.csvFile.Close()
}
