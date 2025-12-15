package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const SLURS_JSON = "flags.json"

var LEET_TABLE = map[rune][]string{
	'a': {"a", "4", "@", "ª"},
	'b': {"b", "8", "6"},
	'c': {"c", "<", "(", "{", "[", "¢"},
	'd': {"d"},
	'e': {"e", "3", "€"},
	'f': {"f"},
	'g': {"g", "9", "6"},
	'h': {"h", "#"},
	'i': {"i", "1", "!", "l", "|"},
	'j': {"j"},
	'k': {"k"},
	'l': {"l", "1", "|", "¡"},
	'm': {"m"},
	'n': {"n"},
	'o': {"o", "0", "()"},
	'p': {"p"},
	'q': {"q", "9"},
	'r': {"r"},
	's': {"s", "5", "$"},
	't': {"t", "7", "+"},
	'u': {"u", "v"},
	'v': {"v", "\\/"},
	'w': {"w", "\\/\\/"},
	'x': {"x", "%", "*"},
	'y': {"y"},
	'z': {"z", "2"},
}

func utcNowISO() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05Z")
}

func headerBlock(count int) string {
	return fmt.Sprintf(
		"\"\nLeaderboard Scan taken @ %s in UTC \nAmount of Flagged Accounts in file: %d\nAuthor of the Filter: Simon\n\"\n\n",
		utcNowISO(),
		count,
	)
}

func asciiFold(s string) string {
	t := norm.NFD.String(s)
	var b strings.Builder
	for _, r := range t {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		if r < utf8.RuneSelf {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func findDataWWW() (string, bool) {
	cwd, _ := os.Getwd()
	dir := cwd
	for {
		candidate := filepath.Join(dir, "data", "www")
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}

func fetchSlurs() map[string]struct{} {
	b, err := os.ReadFile(SLURS_JSON)
	if err != nil {
		fmt.Println("flags.json not found")
		os.Exit(1)
	}

	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		fmt.Println("Failed to parse flags.json")
		os.Exit(1)
	}

	out := make(map[string]struct{})

	var walk func(any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for _, x := range t {
				walk(x)
			}
		case []any:
			for _, x := range t {
				walk(x)
			}
		default:
			s := asciiFold(fmt.Sprint(t))
			s = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(s, "")
			if len(s) >= 2 {
				out[s] = struct{}{}
			}
		}
	}

	walk(raw)
	return out
}

func buildSlurPattern(slur string) *regexp.Regexp {
	var parts []string

	for _, r := range slur {
		if variants, ok := LEET_TABLE[r]; ok {
			var escaped []string
			for _, v := range variants {
				escaped = append(escaped, regexp.QuoteMeta(v))
			}
			escaped = append(escaped, regexp.QuoteMeta(string(r)))
			parts = append(parts, "(?:"+strings.Join(escaped, "|")+")")
		} else {
			parts = append(parts, regexp.QuoteMeta(string(r)))
		}
	}

	sep := `[\W_]*`

	pattern :=
		`(?i)(?:^|[^a-z0-9])` +
			strings.Join(parts, sep) +
			`(?:$|[^a-z0-9])`

	return regexp.MustCompile(pattern)
}

func compilePatterns(slurs map[string]struct{}) map[string]*regexp.Regexp {
	out := make(map[string]*regexp.Regexp)
	for s := range slurs {
		out[s] = buildSlurPattern(s)
	}
	return out
}

func usernameCandidates(raw string) []string {
	n := asciiFold(raw)
	collapsed := regexp.MustCompile(`[\W_]+`).ReplaceAllString(n, "")
	spaceless := strings.ReplaceAll(n, " ", "")

	uniq := map[string]struct{}{
		raw:       {},
		n:         {},
		collapsed: {},
		spaceless: {},
	}

	var out []string
	for k := range uniq {
		out = append(out, k)
	}
	return out
}

func detect(username string, patterns map[string]*regexp.Regexp) []string {
	found := make(map[string]struct{})
	for _, cand := range usernameCandidates(username) {
		for k, p := range patterns {
			if p.MatchString(cand) {
				found[k] = struct{}{}
			}
		}
	}
	var out []string
	for k := range found {
		out = append(out, k)
	}
	return out
}

func sanitizeFilename(s string) string {
	s = regexp.MustCompile(`[^a-zA-Z0-9_-]`).ReplaceAllString(s, "_")
	if s == "" {
		return "group"
	}
	return s
}

func writeTxt(path string, lines []string) {
	os.MkdirAll(filepath.Dir(path), 0755)
	f, _ := os.Create(path)
	defer f.Close()

	w := bufio.NewWriter(f)
	w.WriteString(headerBlock(len(lines)))
	for _, l := range lines {
		w.WriteString(l + "\n")
	}
	w.Flush()
}

func main() {
	dataWWW, ok := findDataWWW()
	if !ok {
		fmt.Println("Could not locate data/www")
		os.Exit(1)
	}

	hitsRoot := filepath.Join(filepath.Dir(dataWWW), "Hits")
	slurDir := filepath.Join(hitsRoot, "Inappropriate_words")
	collectionsDir := filepath.Join(hitsRoot, "inappropriate_accounts_collections")

	os.MkdirAll(slurDir, 0755)
	os.MkdirAll(collectionsDir, 0755)

	slurs := fetchSlurs()
	patterns := compilePatterns(slurs)

	var allLines []string
	bySlur := make(map[string][]string)

	filepath.WalkDir(dataWWW, func(path string, d fs.DirEntry, _ error) error {
		if d == nil || !d.IsDir() {
			return nil
		}

		dataFile := filepath.Join(path, "data.json")
		b, err := os.ReadFile(dataFile)
		if err != nil {
			return nil
		}

		var data map[string]any
		if json.Unmarshal(b, &data) != nil {
			return nil
		}

		var batchLines []string

		for _, v := range data {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}

			latest, ok := m["latest"].(map[string]any)
			if !ok {
				continue
			}

			username, _ := latest["username"].(string)
			if username == "" {
				continue
			}

			idFloat, ok := latest["id"].(float64)
			if !ok {
				continue
			}
			profileID := int64(idFloat)

			found := detect(username, patterns)
			if len(found) == 0 {
				continue
			}

			url := fmt.Sprintf("https://www.kogama.com/profile/%d/", profileID)
			line := fmt.Sprintf("%s | %s", url, username)

			batchLines = append(batchLines, line)
			allLines = append(allLines, line)

			for _, s := range found {
				bySlur[s] = append(bySlur[s], line)
			}
		}

		if len(batchLines) > 0 {
			out := filepath.Join(slurDir, sanitizeFilename(filepath.Base(path))+"_slurs.txt")
			writeTxt(out, batchLines)
		}

		return nil
	})

	writeTxt(filepath.Join(hitsRoot, "inappropriate_accounts.txt"), allLines)

	for slur, lines := range bySlur {
		out := filepath.Join(collectionsDir, "txt", "slur_"+sanitizeFilename(slur)+".txt")
		writeTxt(out, lines)
	}

	fmt.Printf("Done. Found %d accounts with slurs.\n", len(allLines))
	fmt.Printf("TXT hits written to %s\n", hitsRoot)
}
