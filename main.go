// main.go

package main

import (
	"fmt"
	"log"
	"os"

	"snowflake/pkg/snowflake"
)

func dir(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func main() {
	data, err := dir("config.yml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	config, err := snowflake.ParseConfigYAML(data)
	if err != nil {
		log.Fatalf("Error parsing config: %v", err)
	}

	fmt.Printf("Loaded config: %+v\n", config)

	generator, err := snowflake.NewGenerator(config)
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	id, err := generator.NextID()
	if err != nil {
		log.Fatalf("Failed to generate ID: %v", err)
	}
	fmt.Printf("Generated ID: %d\n", id)

	timestamp, datacenterID, machineID, sequence := generator.Decode(id)
	fmt.Printf("Decoded -> Timestamp: %d, Datacenter ID: %d, Machine ID: %d, Sequence: %d\n",
		timestamp, datacenterID, machineID, sequence)
}
