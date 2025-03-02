// main.go
package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"snowflake/pkg/snowflake"
	"time"
)

func dir(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func main() {
	data, err := dir("config.yml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	cfg, err := snowflake.ParseConfigYAML(data)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	gen, err := snowflake.NewGenerator(cfg)
	if err != nil {
		log.Fatalf("failed to create generator: %v", err)
	}

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var count int
	start := time.Now()
	for time.Since(start) < time.Second {
		_, err := gen.NextID()
		if err != nil {
			log.Fatalf("failed to generate id: %v", err)
		}
		count++
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&memAfter)

	fmt.Printf("Generated %d IDs in %v\n", count, elapsed)
	fmt.Printf("IDs per second: %d\n", count)
	fmt.Printf("Average time per ID: %v\n", elapsed/time.Duration(count))
	memUsed := memAfter.Alloc - memBefore.Alloc
	fmt.Printf("Memory allocated: %d bytes\n", memUsed)
}
