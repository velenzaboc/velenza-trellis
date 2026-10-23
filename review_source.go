package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type ReviewProducerConfig struct {
	Generation string `json:"generation"`
	Executable string `json:"executable"`
	ExecutableSHA256 string `json:"executable_sha256"`
	Args []string `json:"args"`
	TimeoutMilliseconds int `json:"timeout_milliseconds"`
	MaxOutputBytes int `json:"max_output_bytes"`
	Repositories []ReviewRepositoryConfig `json:"repositories"`
}

type ReviewRepositoryConfig struct {
	Repo string `json:"repo"`
	Path string `json:"path"`
	RequirementsPath string `json:"requirements_path"`
}

var reviewIdentifierPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
var reviewDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var reviewCommitPattern = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

func validateReviewConfiguration(c SpecGraphConfig) error {
	ordinaryTables := map[string]bool{}
	for _, d := range c.Substrates {
		for _, table := range []string{d.TaskTable,d.WorklinkTable,d.DepTable,d.NodeTable,d.EdgeTable,d.AdmissionLogTable} {
			if table != "" { ordinaryTables[strings.ToLower(table)] = true }
		}
	}
	seen := map[string]bool{}
	for _, d := range c.Substrates {
		if d.ReviewEvidenceTable == "" { continue }
		name := strings.ToLower(d.ReviewEvidenceTable)
		// Evidence owns its project key independently of the operational ledger.
		// A pre-cutover ledger still uses resolveNativeProjectID's default-only
		// boundary; configuring review evidence must not require replacing it.
		if !reviewIdentifierPattern.MatchString(d.ReviewEvidenceTable) || ordinaryTables[name] || seen[name] || d.TaskStoreType != fleetPlanNativeStoreType {
			return fmt.Errorf("review_evidence_table must be an exclusive identifier on a native substrate")
		}
		seen[name] = true
	}
	if c.ReviewProducer == nil { return nil }
	p := c.ReviewProducer
	if strings.TrimSpace(p.Generation) == "" || len(p.Generation) > 128 || !filepath.IsAbs(p.Executable) || !reviewDigestPattern.MatchString(p.ExecutableSHA256) || p.TimeoutMilliseconds <= 0 || p.TimeoutMilliseconds > 600000 || p.MaxOutputBytes <= 0 || p.MaxOutputBytes > 16*1024*1024 || len(p.Repositories) == 0 {
		return fmt.Errorf("review_producer requires a generation, absolute pinned executable, bounded positive timeout/output and explicit repositories")
	}
	name := strings.ToLower(filepath.Base(p.Executable))
	for _, shell := range []string{"sh","bash","zsh","dash","cmd","cmd.exe","powershell","powershell.exe","pwsh","pwsh.exe"} {
		if name == shell { return fmt.Errorf("review_producer must execute the pinned reviewer directly, never a shell") }
	}
	toolsDisabled := 0
	for i, arg := range p.Args {
		if strings.IndexByte(arg,0) >= 0 { return fmt.Errorf("invalid review_producer argument") }
		if arg == "--tools" { if i+1 >= len(p.Args) || p.Args[i+1] != "" { return fmt.Errorf("review_producer tools must be disabled") }; toolsDisabled++ }
	}
	if toolsDisabled != 1 { return fmt.Errorf("review_producer fixed args must explicitly contain --tools with one empty value") }
	seen = map[string]bool{}
	for _, repo := range p.Repositories {
		key := nativeNormalizeRepository(repo.Repo)
		if key == "" || seen[key] || !filepath.IsAbs(repo.Path) || !reviewRelativePath(repo.RequirementsPath) { return fmt.Errorf("invalid or ambiguous review_producer repository configuration") }
		seen[key] = true
	}
	return nil
}

func reviewHashBytes(data []byte) string { digest := sha256.Sum256(data); return hex.EncodeToString(digest[:]) }

func reviewJSON(value interface{}) ([]byte,error) { return json.Marshal(value) }

// Legacy hook consumers hash JSON.stringify(the parsed approval). Preserve
// their property order and string bytes, including HTML and U+2028/U+2029.
// Input-bundle digests separately retain the owner-specified json.Marshal form.
func reviewConsumerJSON(value interface{}) ([]byte,error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil { return nil,err }
	encoded := bytes.TrimSuffix(buffer.Bytes(),[]byte{'\n'})
	out := make([]byte,0,len(encoded))
	for i:=0;i<len(encoded); {
		if encoded[i] == '\\' && i+1 < len(encoded) {
			if i+6 <= len(encoded) && (string(encoded[i:i+6]) == `\u2028` || string(encoded[i:i+6]) == `\u2029`) {
				if encoded[i+5] == '8' { out = append(out,[]byte("\u2028")...) } else { out = append(out,[]byte("\u2029")...) }; i += 6; continue
			}
			out = append(out,encoded[i],encoded[i+1]); i += 2; continue
		}
		out = append(out,encoded[i]); i++
	}
	return out,nil
}

func reviewEqual(a,b interface{}) bool {
	x,err := reviewJSON(a); if err != nil { return false }
	y,err := reviewJSON(b); if err != nil { return false }
	var left,right interface{}
	first,second := json.NewDecoder(bytes.NewReader(x)),json.NewDecoder(bytes.NewReader(y))
	first.UseNumber(); second.UseNumber()
	if first.Decode(&left) != nil || second.Decode(&right) != nil { return false }
	return reflect.DeepEqual(left,right)
}

func reviewRelativePath(value string) bool {
	if value == "" || value == "." || !utf8.ValidString(value) || strings.ContainsAny(value,"\\\x00\r\n:") || strings.HasPrefix(value,"/") || path.Clean(value) != value { return false }
	for _, part := range strings.Split(value,"/") { if part == ".." || part == "." || part == "" { return false } }
	return true
}

type reviewSourceFile struct {
	Path string `json:"path"`
	SHA256 string `json:"sha256"`
	Content string `json:"content"`
}

type reviewProductionInput struct {
	BaseRevision string `json:"base_revision"`
	Revision string `json:"revision"`
	Diff string `json:"diff"`
	Files []reviewSourceFile `json:"files"`
}

// Git object acquisition reuses the existing read-only Git environment. Only
// exact object IDs are accepted, never rev expressions or
// working-tree bytes. Replacement objects and external diff/textconv are off.
func reviewGit(ctx context.Context, root string, args ...string) (string,error) {
	cmd := exec.CommandContext(ctx,"git",append([]string{"--no-replace-objects"},args...)...)
	cmd.Dir,cmd.Env = root,fgFreshnessGitEnv(os.Environ())
	budget := &reviewOutputBudget{left:64*1024*1024}
	stdout,stderr := &reviewOutputWriter{budget:budget},&reviewOutputWriter{budget:budget}
	cmd.Stdout,cmd.Stderr,cmd.WaitDelay = stdout,stderr,time.Second
	if err := cmd.Run(); err != nil { return "",fmt.Errorf("verified review Git read failed: %w",err) }
	if budget.exceeded { return "",fmt.Errorf("verified review Git read exceeded bounded output") }
	return stdout.output.String(),nil
}

func reviewGitObject(ctx context.Context,root,kind,object string) (string,error) {
	if !reviewCommitPattern.MatchString(object) { return "",fmt.Errorf("invalid immutable Git object identity") }
	size,err := reviewGit(ctx,root,"cat-file","-s",object)
	if err != nil { return "",err }
	var count int64
	if _,err := fmt.Sscan(strings.TrimSpace(size),&count); err != nil || count < 0 || count > 16*1024*1024 { return "",fmt.Errorf("review Git object exceeds bounded acquisition") }
	body,err := reviewGit(ctx,root,"cat-file",kind,object)
	if err != nil || int64(len(body)) != count { return "",fmt.Errorf("immutable Git object unavailable or truncated") }
	// Recompute the repository's own object address with Git, without choosing
	// another cryptographic scheme. A corrupt/replaced object file is not proof
	// merely because rev-parse or ls-tree printed its claimed pathname/OID.
	cmd := exec.CommandContext(ctx,"git","--no-replace-objects","hash-object","--stdin","-t",kind)
	cmd.Dir,cmd.Env,cmd.Stdin = root,fgFreshnessGitEnv(os.Environ()),strings.NewReader(body)
	actual,err := cmd.Output()
	if err != nil || strings.TrimSpace(string(actual)) != object { return "",fmt.Errorf("immutable Git object address does not match its actual bytes") }
	return body,nil
}

func reviewVerifyCommit(ctx context.Context,root,revision string) error {
	if !reviewCommitPattern.MatchString(revision) { return fmt.Errorf("review revision requires a verified immutable Git object; unverified source provider labels refuse") }
	out,err := reviewGit(ctx,root,"rev-parse","--verify",revision+"^{commit}")
	if err != nil { return err }
	if strings.TrimSpace(out) != revision { return fmt.Errorf("review revision is not the exact committed object") }
	if _,err := reviewGitObject(ctx,root,"commit",revision); err != nil { return err }
	return nil
}

func reviewGitFile(ctx context.Context,root,revision,filePath,expected string) (reviewSourceFile,error) {
	if !reviewRelativePath(filePath) || !reviewDigestPattern.MatchString(expected) { return reviewSourceFile{},fmt.Errorf("invalid frozen review file path/hash") }
	commit,err := reviewGitObject(ctx,root,"commit",revision)
	if err != nil { return reviewSourceFile{},err }
	firstLine,_,_ := strings.Cut(commit,"\n")
	tree,ok := strings.CutPrefix(firstLine,"tree ")
	if !ok { return reviewSourceFile{},fmt.Errorf("committed source has no exact root tree") }
	parts := strings.Split(filePath,"/")
	var entry string
	for index,component := range parts {
		if _,err := reviewGitObject(ctx,root,"tree",tree); err != nil { return reviewSourceFile{},err }
		entry,err = reviewGit(ctx,root,"ls-tree","-z",tree,"--",component)
		if err != nil { return reviewSourceFile{},err }
		if index == len(parts)-1 { break }
		metadata,name,exists := strings.Cut(strings.TrimSuffix(entry,"\x00"),"\t")
		fields := strings.Fields(metadata)
		if !exists || name != component || len(fields) != 3 || fields[0] != "040000" || fields[1] != "tree" { return reviewSourceFile{},fmt.Errorf("frozen source parent is missing or not a committed directory") }
		tree = fields[2]
	}
	entries := strings.Split(entry,"\x00")
	if len(entries) != 2 || entries[1] != "" { return reviewSourceFile{},fmt.Errorf("frozen review source path is missing or ambiguous") }
	metadata,name,ok := strings.Cut(entries[0],"\t")
	fields := strings.Fields(metadata)
	if !ok || name != parts[len(parts)-1] || len(fields) != 3 || (fields[0] != "100644" && fields[0] != "100755") || fields[1] != "blob" { return reviewSourceFile{},fmt.Errorf("review source must be an exact regular committed file, never symlink/submodule/directory") }
	body,err := reviewGitObject(ctx,root,"blob",fields[2])
	if err != nil { return reviewSourceFile{},err }
	if !utf8.ValidString(body) || reviewHashBytes([]byte(body)) != expected { return reviewSourceFile{},fmt.Errorf("frozen review source bytes/hash mismatch") }
	return reviewSourceFile{filePath,expected,body},nil
}

func reviewAcquireFiles(ctx context.Context,root,revision string,raw interface{}) ([]reviewSourceFile,error) {
	files,ok := raw.([]interface{})
	if !ok || len(files) == 0 { return nil,fmt.Errorf("nonempty frozen review file list required") }
	seen := map[string]bool{}
	out := make([]reviewSourceFile,0,len(files))
	var total int
	for _, item := range files {
		entry,ok := item.(map[string]interface{})
		if !ok || len(entry) != 2 { return nil,fmt.Errorf("frozen file entry must contain exactly path and sha256") }
		name,hash := stringFromMap(entry,"path"),stringFromMap(entry,"sha256")
		if seen[name] { return nil,fmt.Errorf("duplicate frozen review file") }; seen[name] = true
		file,err := reviewGitFile(ctx,root,revision,name,hash)
		if err != nil { return nil,err }
		total += len(file.Content)
		if total > 64*1024*1024 { return nil,fmt.Errorf("review input exceeds bounded acquisition") }
		out = append(out,file)
	}
	sort.Slice(out,func(i,j int) bool { return out[i].Path < out[j].Path })
	return out,nil
}

func reviewAcquireProduction(ctx context.Context,root,base,head string,scope interface{}) (reviewProductionInput,error) {
	if err := reviewVerifyCommit(ctx,root,base); err != nil { return reviewProductionInput{},err }
	if err := reviewVerifyCommit(ctx,root,head); err != nil { return reviewProductionInput{},err }
	if base == head { return reviewProductionInput{},fmt.Errorf("production review needs an actual base-to-head change") }
	if _,err := reviewGit(ctx,root,"merge-base","--is-ancestor",base,head); err != nil { return reviewProductionInput{},fmt.Errorf("production base/head lineage is unverified: %w",err) }
	names,err := reviewGit(ctx,root,"diff","--no-ext-diff","--no-textconv","--no-renames","--name-only","-z",base,head,"--")
	if err != nil { return reviewProductionInput{},err }
	paths := strings.Split(strings.TrimSuffix(names,"\x00"),"\x00")
	declared,ok := scope.([]interface{})
	if !ok || len(declared) == 0 || len(paths) != len(declared) { return reviewProductionInput{},fmt.Errorf("complete declared production scope required") }
	byPath := map[string]string{}
	for _, raw := range declared {
		file,ok := raw.(map[string]interface{})
		if !ok || len(file) != 2 { return reviewProductionInput{},fmt.Errorf("invalid production scope") }
		name,hash := stringFromMap(file,"path"),stringFromMap(file,"sha256")
		if _,dup := byPath[name]; dup || !reviewRelativePath(name) || !reviewDigestPattern.MatchString(hash) { return reviewProductionInput{},fmt.Errorf("invalid/duplicate production scope") }
		byPath[name] = hash
	}
	out := reviewProductionInput{BaseRevision:base,Revision:head,Files:[]reviewSourceFile{}}
	var total int
	for _, name := range paths {
		hash,exists := byPath[name]
		if !exists { return reviewProductionInput{},fmt.Errorf("undeclared changed production file") }
		file,err := reviewGitFile(ctx,root,head,name,hash)
		if err != nil {
			// A deletion's full committed body is at the base, with the diff
			// carrying its deletion semantics. Never substitute a local file.
			entry,readErr := reviewGit(ctx,root,"ls-tree","-z",head,"--",name)
			if readErr != nil || entry != "" { return reviewProductionInput{},err }
			file,err = reviewGitFile(ctx,root,base,name,hash)
			if err != nil { return reviewProductionInput{},err }
		}
		total += len(file.Content)
		if total > 64*1024*1024 { return reviewProductionInput{},fmt.Errorf("production input exceeds bounded acquisition") }
		out.Files = append(out.Files,file)
	}
	out.Diff,err = reviewGit(ctx,root,"diff","--no-ext-diff","--no-textconv","--no-renames","--binary",base,head,"--")
	if err != nil || out.Diff == "" { return reviewProductionInput{},fmt.Errorf("complete production diff unavailable: %w",err) }
	return out,nil
}

func reviewProgram(config *ReviewProducerConfig) ([]byte,error) {
	if config == nil { return nil,fmt.Errorf("review producer capability is not configured") }
	file,err := os.Open(config.Executable)
	if err != nil { return nil,fmt.Errorf("pinned review executable unavailable: %w",err) }
	defer file.Close()
	stat,err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > 512*1024*1024 { return nil,fmt.Errorf("review executable must be a bounded regular file") }
	program,err := io.ReadAll(io.LimitReader(file,512*1024*1024+1))
	if err != nil || reviewHashBytes(program) != config.ExecutableSHA256 { return nil,fmt.Errorf("review executable pin mismatch") }
	return program,nil
}

type reviewCapturedProcess struct {
	Generation string `json:"generation"`
	ExecutableSHA256 string `json:"executable_sha256"`
	Args []string `json:"args"`
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	ExitCode int `json:"exit_code"`
}

type reviewOutputBudget struct { mu sync.Mutex; left int; exceeded bool }
type reviewOutputWriter struct { budget *reviewOutputBudget; output bytes.Buffer }
func (w *reviewOutputWriter) Write(data []byte) (int,error) {
	w.budget.mu.Lock(); defer w.budget.mu.Unlock()
	if len(data) > w.budget.left { w.budget.exceeded = true; return 0,fmt.Errorf("review output limit exceeded") }
	w.budget.left -= len(data)
	return w.output.Write(data)
}

func reviewRunProcess(ctx context.Context,config *ReviewProducerConfig,program,input []byte) (reviewCapturedProcess,error) {
	// Run the verified bytes from an exclusive private snapshot. A replace of
	// the configured pathname after hash verification cannot change this child.
	dir,err := os.MkdirTemp("","task-graph-review-")
	if err != nil { return reviewCapturedProcess{},err }
	defer os.RemoveAll(dir)
	name := filepath.Join(dir,"reviewer"+filepath.Ext(config.Executable))
	if err := os.WriteFile(name,program,0700); err != nil { return reviewCapturedProcess{},err }
	deadline,cancel := context.WithTimeout(ctx,time.Duration(config.TimeoutMilliseconds)*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(deadline,name,config.Args...)
	cmd.Dir = dir
	// Only service-owned OS/runtime coordinates reach the child. In particular
	// MCP arguments, repository config and inherited auth/token variables do not.
	cmd.Env = []string{}
	// USER is an OS identity coordinate used by the native reviewer's existing
	// keychain lookup; it is not a credential or caller-selected override.
	for _, key := range []string{"PATH","HOME","USER","USERPROFILE","SYSTEMROOT","WINDIR","TMPDIR","TMP","TEMP","LANG","LC_ALL"} {
		if value,ok := os.LookupEnv(key); ok { cmd.Env = append(cmd.Env,key+"="+value) }
	}
	cmd.Stdin = bytes.NewReader(input)
	budget := &reviewOutputBudget{left:config.MaxOutputBytes}
	stdout,stderr := &reviewOutputWriter{budget:budget},&reviewOutputWriter{budget:budget}
	cmd.Stdout,cmd.Stderr = stdout,stderr
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if err != nil || deadline.Err() != nil || budget.exceeded { return reviewCapturedProcess{},fmt.Errorf("review process failed/timed out/exceeded output: %w",err) }
	return reviewCapturedProcess{config.Generation,config.ExecutableSHA256,append([]string{},config.Args...),stdout.output.String(),stderr.output.String(),0},nil
}

type reviewCoverage struct {
	Requirement string `json:"requirement"`
	Path string `json:"path"`
	Case string `json:"case"`
	Assertion string `json:"assertion"`
	Boundary string `json:"boundary"`
	Scenario string `json:"scenario"`
}
type reviewChildResult struct {
	SchemaVersion string `json:"schema_version"`
	Kind string `json:"kind"`
	InputSHA256 string `json:"input_sha256"`
	Requirements []string `json:"requirements"`
	Coverage []reviewCoverage `json:"coverage"`
	Blocking []json.RawMessage `json:"blocking"`
	ProductionBoundary *bool `json:"production_boundary"`
	Tautologies *bool `json:"tautologies"`
	FixtureSuppliedAuthority *bool `json:"fixture_supplied_authority"`
	Summary string `json:"summary"`
	ProductionBaseRevision string `json:"production_base_revision,omitempty"`
	ProductionRevision string `json:"production_revision,omitempty"`
	ReviewedFiles []struct { Path string `json:"path"`; SHA256 string `json:"sha256"` } `json:"reviewed_files,omitempty"`
}

func reviewValidateResult(raw string,bundle map[string]interface{},tests []reviewSourceFile,requirements []string,production *reviewProductionInput) (map[string]interface{},error) {
	if err := reviewClosedFields([]byte(raw),map[string]bool{"schema_version":true,"kind":true,"input_sha256":true,"requirements":true,"coverage":true,"blocking":true,"production_boundary":true,"tautologies":true,"fixture_supplied_authority":true,"summary":true,"production_base_revision":true,"production_revision":true,"reviewed_files":true}); err != nil { return nil,err }
	var result reviewChildResult
	if err := reviewStrictJSON([]byte(raw),&result); err != nil { return nil,fmt.Errorf("invalid strict review result: %w",err) }
	if result.SchemaVersion != "velenza_independent_review/1" || result.Kind != bundle["kind"] || result.InputSHA256 != bundle["input_sha256"] || result.Blocking == nil || len(result.Blocking) != 0 || result.ProductionBoundary == nil || !*result.ProductionBoundary || result.Tautologies == nil || *result.Tautologies || result.FixtureSuppliedAuthority == nil || *result.FixtureSuppliedAuthority || strings.TrimSpace(result.Summary) == "" {
		return nil,fmt.Errorf("review failed, blocked or has incomplete boundary evidence")
	}
	wanted := map[string]bool{}
	for _, req := range requirements { wanted[req] = true }
	seen := map[string]bool{}
	for _, req := range result.Requirements { if !wanted[req] || seen[req] { return nil,fmt.Errorf("review requirements are incomplete/ambiguous") }; seen[req] = true }
	if len(seen) != len(wanted) { return nil,fmt.Errorf("review omitted a governing requirement") }
	fileByPath := map[string]reviewSourceFile{}
	for _, file := range tests { fileByPath[file.Path] = file }
	scenarios,covered := map[string]bool{},map[string]bool{}
	for _, coverage := range result.Coverage {
		file,exists := fileByPath[coverage.Path]
		if !wanted[coverage.Requirement] || !exists || strings.TrimSpace(coverage.Case) == "" || strings.TrimSpace(coverage.Assertion) == "" || strings.TrimSpace(coverage.Boundary) == "" || !strings.Contains(file.Content,coverage.Case) || !strings.Contains(file.Content,coverage.Assertion) { return nil,fmt.Errorf("review coverage does not name actual committed assertions and boundary") }
		if coverage.Scenario != "positive" && coverage.Scenario != "refusal" && coverage.Scenario != "exception" { return nil,fmt.Errorf("review coverage scenario is incomplete") }
		scenarios[coverage.Requirement+":"+coverage.Scenario] = true
		covered[file.Path] = true
	}
	for req := range wanted { for _, scenario := range []string{"positive","refusal","exception"} { if !scenarios[req+":"+scenario] { return nil,fmt.Errorf("review omitted requirement scenario %s/%s",req,scenario) } } }
	if len(covered) != len(tests) { return nil,fmt.Errorf("review omitted a frozen executable test file") }
	if production != nil {
		if result.ProductionBaseRevision != production.BaseRevision || result.ProductionRevision != production.Revision || len(result.ReviewedFiles) != len(production.Files) { return nil,fmt.Errorf("production review omitted actual head/base/files") }
		actual := map[string]string{}
		for _, file := range production.Files { actual[file.Path] = file.SHA256 }
		for _, file := range result.ReviewedFiles { if actual[file.Path] != file.SHA256 { return nil,fmt.Errorf("production reviewed-file identity mismatch") }; delete(actual,file.Path) }
		if len(actual) != 0 { return nil,fmt.Errorf("production review scope incomplete") }
	} else if result.ProductionBaseRevision != "" || result.ProductionRevision != "" || result.ReviewedFiles != nil { return nil,fmt.Errorf("validation review cannot substitute production evidence") }
	var out map[string]interface{}
	if err := reviewStrictJSON([]byte(raw),&out); err != nil { return nil,err }
	return out,nil
}

// Keep comparisons of a captured invocation explicit. Stored caller-shaped
// JSON is not execution origin; only the typed producer inserts this envelope.
func reviewProcessMatches(captured reviewCapturedProcess,config *ReviewProducerConfig) bool {
	return captured.ExitCode == 0 && captured.Generation == config.Generation && captured.ExecutableSHA256 == config.ExecutableSHA256 && reflect.DeepEqual(captured.Args,config.Args)
}
