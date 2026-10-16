// Command streamx is the STREAMX CLI.
//
// Subcommands:
//
//	streamx topic create <name> [--partitions N] [--rf N]
//	streamx topic list
//	streamx produce <topic> [--count N] [--key-pattern "cust-{n}"] [--payload '{...}'] [--acks leader]
//	streamx consume <topic> [--group G] [--handler log] [--duration 30s]
//	streamx bench [--events N] [--payload B] [--producers P] [--consumers C] [--partitions N]
//	streamx failure broker <id> <stop|restart|kill>
//
// Configuration (env):
//
//	STREAMX_CLUSTER   id=host:port,... (default local 3-broker cluster)
//	STREAMX_API_URL   control-plane URL (default http://localhost:8080)
//	STREAMX_API_TOKEN bearer token (optional; auto-login as admin if absent)
//	STREAMX_ADMIN_USER / STREAMX_ADMIN_PASSWORD  (default admin/admin123)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streamx/streamx/internal/bench"
	"github.com/streamx/streamx/internal/config"
	"github.com/streamx/streamx/internal/protocol"
	sdk "github.com/streamx/streamx/sdk/streamx"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "topic":
		err = cmdTopic(args)
	case "produce":
		err = cmdProduce(args)
	case "consume":
		err = cmdConsume(args)
	case "bench":
		err = cmdBench(args)
	case "failure":
		err = cmdFailure(args)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `streamx — STREAMX CLI

Usage:
  streamx topic create <name> [--partitions N] [--rf N]
  streamx topic list
  streamx produce <topic> [--count N] [--key-pattern "cust-{n}"] [--payload '{...}'] [--acks leader]
  streamx consume <topic> [--group G] [--handler log] [--duration 30s]
  streamx bench [--events N] [--payload B] [--producers P] [--consumers C] [--partitions N]
  streamx failure broker <id> <stop|restart|kill>`)
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

func cluster() map[string]string {
	return config.ParseCluster(config.Get("STREAMX_CLUSTER",
		"broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094"))
}

func apiURL() string { return config.Get("STREAMX_API_URL", "http://localhost:8080") }

// token returns the API token, auto-logging in as admin if none is set.
func token() string {
	if t := config.Get("STREAMX_API_TOKEN", ""); t != "" {
		return t
	}
	u := config.Get("STREAMX_ADMIN_USER", "admin")
	p := config.Get("STREAMX_ADMIN_PASSWORD", "admin123")
	tok, err := apiLogin(u, p)
	if err != nil {
		fmt.Fprintln(os.Stderr, "warn: could not auto-login:", err)
		return ""
	}
	return tok
}

func newClient() (*sdk.Client, error) {
	return sdk.NewClient(sdk.Config{
		Brokers:        cluster(),
		ProducerID:     "cli-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		RequestTimeout: 10 * time.Second,
	})
}

// ---------------------------------------------------------------------------
// topic
// ---------------------------------------------------------------------------

func cmdTopic(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: streamx topic <create|list> ...")
	}
	switch args[0] {
	case "create":
		// Split the topic name (first non-flag) from the flags.
		var name string
		var flags []string
		for _, a := range args[1:] {
			if strings.HasPrefix(a, "-") {
				flags = append(flags, a)
			} else if name == "" {
				name = a
			}
		}
		fs := flag.NewFlagSet("topic create", flag.ExitOnError)
		fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: streamx topic create <name> [--partitions N] [--rf N]") }
		partitions := fs.Int("partitions", 3, "number of partitions")
		rf := fs.Int("rf", 1, "replication factor")
		_ = fs.Parse(flags)
		if name == "" {
			return fmt.Errorf("topic name required")
		}
		return createTopic(name, *partitions, *rf)
	case "list":
		return listTopics()
	default:
		return fmt.Errorf("unknown topic subcommand: %s", args[0])
	}
}

func createTopic(name string, partitions, rf int) error {
	tok := token()
	if tok == "" {
		return fmt.Errorf("no API token (set STREAMX_API_TOKEN or admin credentials)")
	}
	body, _ := json.Marshal(map[string]any{
		"name":               name,
		"partitions":         partitions,
		"replication_factor": rf,
	})
	resp, err := apiPost("/api/v1/topics", body, tok)
	if err != nil {
		return err
	}
	if resp.StatusCode == 409 {
		fmt.Printf("topic %q already exists\n", name)
		return nil
	}
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return fmt.Errorf("create topic status %d: %s", resp.StatusCode, resp.Body)
	}
	fmt.Printf("created topic %q (partitions=%d, rf=%d)\n", name, partitions, rf)
	return nil
}

func listTopics() error {
	tok := token()
	if tok == "" {
		return fmt.Errorf("no API token")
	}
	resp, err := apiGet("/api/v1/topics", tok)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var topics []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		return err
	}
	if len(topics) == 0 {
		fmt.Println("(no topics)")
		return nil
	}
	for _, t := range topics {
		fmt.Printf("  %-32s partitions=%v rf=%v\n", t["name"], t["partitions"], t["replication_factor"])
	}
	return nil
}

// ---------------------------------------------------------------------------
// produce
// ---------------------------------------------------------------------------

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: streamx produce <topic> [--count N] [--key-pattern "cust-{n}"] [--payload '{...}'] [--acks leader]`)
	}
	count := fs.Int("count", 1, "number of events to produce")
	keyPattern := fs.String("key-pattern", "", "key template with {n} placeholder (empty = round-robin)")
	payload := fs.String("payload", "{}", "payload (JSON string or raw)")
	acks := fs.String("acks", "leader", "acks: none|leader|all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("topic name required")
	}
	topic := fs.Arg(0)
	client, err := newClient()
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	payloadBytes := []byte(*payload)
	total := 0
	batch := make([]*protocol.Event, 0, 100)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		resp, err := client.Produce(ctx, topic, *acks, batch)
		if err != nil {
			return err
		}
		for _, r := range resp.Results {
			if r.Error != "" {
				fmt.Fprintln(os.Stderr, "  produce error p"+strconv.Itoa(int(r.Partition))+":", r.Error)
			} else {
				total++
			}
		}
		batch = batch[:0]
		return nil
	}
	for n := 0; n < *count; n++ {
		key := ""
		if *keyPattern != "" {
			key = strings.ReplaceAll(*keyPattern, "{n}", strconv.Itoa(n))
		}
		batch = append(batch, &protocol.Event{Key: key, Payload: payloadBytes})
		if len(batch) >= 100 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	fmt.Printf("produced %d events to %q (acks=%s)\n", total, topic, *acks)
	return nil
}

// ---------------------------------------------------------------------------
// consume
// ---------------------------------------------------------------------------

func cmdConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: streamx consume <topic> [--group G] [--handler log] [--duration 30s]`)
	}
	group := fs.String("group", "cli-group", "consumer group")
	handler := fs.String("handler", "log", "handler: noop|log|fail:<p>|slow:<ms>")
	duration := fs.Duration("duration", 30*time.Second, "how long to consume")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("topic name required")
	}
	topic := fs.Arg(0)
	client, err := newClient()
	if err != nil {
		return err
	}
	defer client.Close()
	var count int64
	consumer, err := sdk.NewConsumer(sdk.GroupConfig{
		Client:  client,
		Group:   *group,
		Topic:   topic,
		StartAt: "earliest",
		Handler: func(ctx context.Context, ev *protocol.Event) error {
			count++
			if *handler == "log" {
				fmt.Printf("  [%s] p%d off=%d key=%q\n", topic, ev.Partition, ev.Offset, ev.Key)
			}
			return nil
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	fmt.Printf("consuming from %q (group=%s) for %s…\n", topic, *group, *duration)
	_ = consumer.Start(ctx)
	fmt.Printf("consumed %d events from %q\n", count, topic)
	return nil
}

// ---------------------------------------------------------------------------
// bench
// ---------------------------------------------------------------------------

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: streamx bench [--events N] [--payload B] [--producers P] [--consumers C] [--partitions N]`)
	}
	events := fs.Int("events", 10000, "number of events")
	payload := fs.Int("payload", 512, "payload bytes")
	producers := fs.Int("producers", 2, "number of producers")
	consumers := fs.Int("consumers", 2, "number of consumers")
	partitions := fs.Int("partitions", 3, "number of partitions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := bench.Config{
		Brokers:      cluster(),
		APIURL:       apiURL(),
		APIToken:     token(),
		Scenario:     "throughput",
		Events:       *events,
		PayloadBytes: *payload,
		Producers:    *producers,
		Consumers:    *consumers,
		Partitions:   *partitions,
		Concurrency:  8,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	res, err := bench.Run(ctx, cfg)
	if err != nil {
		return err
	}
	w := 40
	fmt.Println()
	fmt.Println(strings.Repeat("=", w))
	fmt.Println("  STREAMX benchmark result")
	fmt.Println(strings.Repeat("=", w))
	fmt.Printf("  events:            %d\n", res.Produced)
	fmt.Printf("  consumed:          %d\n", res.Consumed)
	fmt.Printf("  duration:          %d ms\n", res.DurationMS)
	fmt.Printf("  throughput:        %.0f events/s (%.0f bytes/s)\n", res.ThroughputEventsPerSec, res.ThroughputBytesPerSec)
	fmt.Printf("  producer p50/p95/p99: %.2f / %.2f / %.2f ms\n", res.ProducerLatencyMS.P50, res.ProducerLatencyMS.P95, res.ProducerLatencyMS.P99)
	fmt.Printf("  consumer p50/p95/p99: %.2f / %.2f / %.2f ms\n", res.ConsumerLatencyMS.P50, res.ConsumerLatencyMS.P95, res.ConsumerLatencyMS.P99)
	fmt.Printf("  errors:            %d (%.3f%%)\n", res.Errors, res.ErrorRate*100)
	fmt.Println(strings.Repeat("=", w))
	if cfg.APIToken != "" {
		if runID, err := registerBenchmark(cfg, res); err != nil {
			fmt.Fprintln(os.Stderr, "  (note: could not register with API:", err, ")")
		} else {
			fmt.Printf("  registered as run %s\n", runID)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// failure
// ---------------------------------------------------------------------------

func cmdFailure(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: streamx failure broker <id> <stop|restart|kill>")
	}
	if args[0] != "broker" {
		return fmt.Errorf("only 'broker' failures are supported from the CLI")
	}
	id, action := args[1], args[2]
	tok := token()
	if tok == "" {
		return fmt.Errorf("no API token")
	}
	body, _ := json.Marshal(map[string]any{"broker_id": id, "action": action})
	resp, err := apiPost("/api/v1/admin/failures/broker", body, tok)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failure status %d: %s", resp.StatusCode, resp.Body)
	}
	fmt.Printf("injected %s failure on broker %s\n", action, id)
	return nil
}
