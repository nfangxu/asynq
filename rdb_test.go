package asynq

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

var client *redis.Client

func init() {
	rand.Seed(time.Now().UnixNano())
}

// setup connects to a redis database and flush all keys
// before returning an instance of rdb.
func setup(t *testing.T) *rdb {
	t.Helper()
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // use database 15 to separate from other applications
	})
	// Start each test with a clean slate.
	if err := client.FlushDB().Err(); err != nil {
		panic(err)
	}
	return newRDB(client)
}

func randomTask(taskType, qname string, payload map[string]interface{}) *taskMessage {
	return &taskMessage{
		ID:      uuid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   rand.Intn(100),
		Payload: make(map[string]interface{}),
	}
}

func TestPush(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *taskMessage
	}{
		{msg: randomTask("send_email", "default", map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{msg: randomTask("generate_csv", "default", map[string]interface{}{})},
		{msg: randomTask("sync", "default", nil)},
	}

	for _, tc := range tests {
		err := r.push(tc.msg)
		if err != nil {
			t.Error(err)
		}
		res := client.LRange(defaultQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("LIST %q has length %d, want 1", defaultQueue, len(res))
			continue
		}
		if !client.SIsMember(allQueues, defaultQueue).Val() {
			t.Errorf("SISMEMBER %q %q = false, want true", allQueues, defaultQueue)
		}
		var persisted taskMessage
		if err := json.Unmarshal([]byte(res[0]), &persisted); err != nil {
			t.Error(err)
			continue
		}
		if diff := cmp.Diff(*tc.msg, persisted); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
		// clean up before the next test case.
		if err := client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDequeueImmediateReturn(t *testing.T) {
	r := setup(t)
	msg := randomTask("export_csv", "csv", nil)
	r.push(msg)

	res, err := r.dequeue("asynq:queues:csv", time.Second)
	if err != nil {
		t.Fatalf("r.bpop() failed: %v", err)
	}

	if !cmp.Equal(res, msg) {
		t.Errorf("cmp.Equal(res, msg) = %t, want %t", false, true)
	}
	jobs := client.LRange(inProgress, 0, -1).Val()
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want %d", len(jobs), 1)
	}
	var tm taskMessage
	if err := json.Unmarshal([]byte(jobs[0]), &tm); err != nil {
		t.Fatalf("json.Marshal() failed: %v", err)
	}
	if diff := cmp.Diff(res, &tm); diff != "" {
		t.Errorf("cmp.Diff(res, tm) = %s", diff)
	}
}

func TestDequeueTimeout(t *testing.T) {
	r := setup(t)

	_, err := r.dequeue("asynq:queues:default", time.Second)
	if err != errQueuePopTimeout {
		t.Errorf("err = %v, want %v", err, errQueuePopTimeout)
	}
}

func TestMoveAll(t *testing.T) {
	r := setup(t)
	seed := []*taskMessage{
		randomTask("send_email", "default", nil),
		randomTask("export_csv", "csv", nil),
		randomTask("sync_stuff", "sync", nil),
	}
	for _, task := range seed {
		bytes, err := json.Marshal(task)
		if err != nil {
			t.Errorf("json.Marhsal() failed: %v", err)
		}
		if err := client.LPush(inProgress, string(bytes)).Err(); err != nil {
			t.Errorf("LPUSH %q %s failed: %v", inProgress, string(bytes), err)
		}
	}

	err := r.moveAll(inProgress, defaultQueue)
	if err != nil {
		t.Errorf("moveAll failed: %v", err)
	}

	if l := client.LLen(inProgress).Val(); l != 0 {
		t.Errorf("LLEN %q = %d, want 0", inProgress, l)
	}
	if l := client.LLen(defaultQueue).Val(); int(l) != len(seed) {
		t.Errorf("LLEN %q = %d, want %d", defaultQueue, l, len(seed))
	}
}

func TestForward(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", defaultQueue, nil)
	t2 := randomTask("generate_csv", defaultQueue, nil)
	secondAgo := time.Now().Add(-time.Second) // use timestamp for the past to avoid advancing time
	json1, err := json.Marshal(t1)
	if err != nil {
		t.Fatalf("json.Marshal() failed: %v", err)
	}
	json2, err := json.Marshal(t2)
	if err != nil {
		t.Fatalf("json.Marshal() failed: %v", err)
	}
	client.ZAdd(scheduled, &redis.Z{
		Member: string(json1),
		Score:  float64(secondAgo.Unix()),
	}, &redis.Z{
		Member: string(json2),
		Score:  float64(secondAgo.Unix()),
	})

	err = r.forward(scheduled)
	if err != nil {
		t.Fatalf("r.forward() failed: %v", err)
	}

	if c := client.ZCard(scheduled).Val(); c != 0 {
		t.Errorf("ZCARD %q = %d, want 0", scheduled, c)
	}
	if l := client.LLen(defaultQueue).Val(); l != 2 {
		t.Errorf("LLEN %q = %d, want 2", defaultQueue, l)
	}
	if c := client.SCard(allQueues).Val(); c != 1 {
		t.Errorf("SCARD %q = %d, want 1", allQueues, c)
	}
}
