package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var HOSTNAMES = map[string]string{
	"www":     "https://www.kogama.com/",
	"br":      "https://www.kogama.com.br/",
	"friends": "https://friends.kogama.com/",
}

const (
	ENDPOINT        = "api/leaderboard/top/"
	COUNT           = 400
	REQUEST_TIMEOUT = 10 * time.Second

	WORKERS        = 6
	PREFETCH_PAGES = 12
	BUCKET_SIZE    = 20000
	SAVE_INTERVAL  = 30 * time.Second
)

type RetryClient struct {
	Client  *http.Client
	Retries int
}

func (rc *RetryClient) Get(url string) (*http.Response, error) {
	var lastErr error
	for i := 0; i < rc.Retries; i++ {
		resp, err := rc.Client.Get(url)
		if err == nil && resp.StatusCode < 500 && resp.StatusCode != 429 {
			return resp, nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		lastErr = err
		time.Sleep(time.Duration(i+1) * 800 * time.Millisecond)
	}
	return nil, lastErr
}

func atomicWrite(path string, obj any) error {
	dir := filepath.Dir(path)
	_ = os.MkdirAll(dir, 0755)

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(obj); err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, &buf); err != nil {
		f.Close()
		return err
	}
	_ = f.Sync()
	f.Close()

	return os.Rename(tmp, path)
}

func loadJSON(path string, dst any) {
	b, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(b, dst)
	}
}

func buildURL(base string, page int) string {
	return fmt.Sprintf(
		"%s/%s?count=%d&page=%d",
		strings.TrimRight(base, "/"),
		ENDPOINT,
		COUNT,
		page,
	)
}

func normalizeID(m map[string]any) string {
	for _, k := range []string{
		"id", "profile_id", "user_id", "player_id",
		"profileId", "playerId", "id_str",
	} {
		if v, ok := m[k]; ok && v != nil {
			return fmt.Sprint(v)
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func rankBucket(rank int) (int, int) {
	if rank <= 0 {
		return 0, 0
	}
	start := ((rank-1)/BUCKET_SIZE)*BUCKET_SIZE + 1
	return start, start + BUCKET_SIZE - 1
}

type Bucket struct {
	Data  map[string]any
	Dirty bool
}

type BucketManager struct {
	root  string
	cache map[[2]int]*Bucket
}

func NewBucketManager(root string) *BucketManager {
	return &BucketManager{
		root:  root,
		cache: make(map[[2]int]*Bucket),
	}
}

func (bm *BucketManager) get(start, end int) *Bucket {
	key := [2]int{start, end}
	if b, ok := bm.cache[key]; ok {
		return b
	}

	path := filepath.Join(bm.root, fmt.Sprintf("%dto%d", start, end), "data.json")
	data := make(map[string]any)
	loadJSON(path, &data)

	b := &Bucket{Data: data}
	bm.cache[key] = b
	return b
}

func extractPages(v any) []int {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]int, 0, len(raw))
	for _, p := range raw {
		switch t := p.(type) {
		case float64:
			out = append(out, int(t))
		case int:
			out = append(out, t)
		case string:
			if n, err := strconv.Atoi(t); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

func (bm *BucketManager) Update(uid string, latest map[string]any, page int) {
	rank := 0
	if v, ok := latest["rank"]; ok {
		switch t := v.(type) {
		case float64:
			rank = int(t)
		case int:
			rank = t
		case string:
			rank, _ = strconv.Atoi(t)
		}
	}

	start, end := rankBucket(rank)
	b := bm.get(start, end)

	var pages []int
	if entry, ok := b.Data[uid].(map[string]any); ok {
		pages = extractPages(entry["pages"])
	}

	for _, p := range pages {
		if p == page {
			goto STORE
		}
	}
	pages = append(pages, page)

STORE:
	b.Data[uid] = map[string]any{
		"latest": latest,
		"pages":  pages,
	}
	b.Dirty = true
}

func (bm *BucketManager) SaveDirty() {
	for key, b := range bm.cache {
		if !b.Dirty {
			continue
		}
		path := filepath.Join(
			bm.root,
			fmt.Sprintf("%dto%d", key[0], key[1]),
			"data.json",
		)
		_ = atomicWrite(path, b.Data)
		b.Dirty = false
	}
}

func fetchPage(client *RetryClient, url string) ([]map[string]any, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	data, _ := raw["data"].([]any)
	out := make([]map[string]any, 0, len(data))
	for _, e := range data {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func run(server string) error {
	outdir := filepath.Join("Data", server)
	_ = os.MkdirAll(outdir, 0755)

	lastPath := filepath.Join(outdir, "last.json")
	last := map[string]any{"page": 1}
	loadJSON(lastPath, &last)

	page := 1
	if v, ok := last["page"]; ok {
		switch t := v.(type) {
		case float64:
			page = int(t)
		case int:
			page = t
		}
	}

	client := &RetryClient{
		Client:  &http.Client{Timeout: REQUEST_TIMEOUT},
		Retries: 5,
	}

	buckets := NewBucketManager(outdir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pageCh := make(chan int, PREFETCH_PAGES)
	dataCh := make(chan []map[string]any, PREFETCH_PAGES)

	var wg sync.WaitGroup

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range pageCh {
				url := buildURL(HOSTNAMES[server], p)
				data, err := fetchPage(client, url)
				if err == nil && len(data) > 0 {
					dataCh <- data
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(dataCh)
	}()

	ticker := time.NewTicker(SAVE_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(pageCh)
			buckets.SaveDirty()
			_ = atomicWrite(lastPath, last)
			return nil

		case pageCh <- page:
			page++
			last["page"] = page

		case data := <-dataCh:
			for _, ent := range data {
				delete(ent, "history")
				buckets.Update(normalizeID(ent), ent, page-1)
			}

		case <-ticker.C:
			buckets.SaveDirty()
			_ = atomicWrite(lastPath, last)
		}
	}
}

func main() {
	fmt.Print("Enter server [br,www,friends]: ")
	var s string
	fmt.Scanln(&s)
	s = strings.ToLower(strings.TrimSpace(s))

	if _, ok := HOSTNAMES[s]; !ok {
		fmt.Println("Invalid server")
		return
	}

	if err := run(s); err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("Finished.")
}
