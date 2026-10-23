package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Use the actual configuration envelope and validator. This helper supplies no
// reviewer result and performs no database or process execution.
func rnConfiguration(t *testing.T, project string, keyed bool, database, repository, executable, control, capture string) map[string]any {
	t.Helper()
	data, err := os.ReadFile("task-graph.post-cutover.config.json")
	if err != nil { t.Fatal(err) }
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil { t.Fatal(err) }
	program, err := os.ReadFile(executable)
	if err != nil { t.Fatal(err) }
	cfg["embedding"] = nil
	cfg["default_project_id"] = project
	cfg["allow_spanner_emulator"] = true
	sub := cfg["substrates"].([]any)[0].(map[string]any)
	sub["spanner_database"] = database
	sub["review_evidence_table"] = "IndependentReviewEvidence"
	if keyed { sub["project_column"] = "project_id" } else { delete(sub,"project_column") }
	cfg["review_producer"] = map[string]any{
		"generation":"native-compat-e1", "executable":executable, "executable_sha256":rpDigest(program),
		"args":[]string{"-test.run=^TestReviewProducerChild$","--","review-process-child",control,capture,"--tools",""},
		"timeout_milliseconds":1000, "max_output_bytes":65536,
		"repositories":[]any{map[string]any{"repo":"example-infra","path":repository,"requirements_path":"REQUIREMENTS.md"}},
	}
	return cfg
}

func rnValidate(t *testing.T, raw map[string]any) error {
	t.Helper()
	var cfg SpecGraphConfig
	if err := json.Unmarshal(rpJSON(t,raw),&cfg); err != nil { t.Fatal(err) }
	return cfg.Validate()
}

func TestReviewNativeCompatibilityConfiguration(t *testing.T) {
	executable, err := os.Executable()
	if err != nil { t.Fatal(err) }
	for _, project := range []string{"baos","orchard-native"} {
		for _, keyed := range []bool{false,true} {
			name := project+"/legacy"; if keyed { name=project+"/project-keyed" }
			t.Run(name,func(t *testing.T){
				dir:=t.TempDir()
				cfg:=rnConfiguration(t,project,keyed,"projects/test-project/instances/test-instance/databases/test-corpus-graph",dir,executable,filepath.Join(dir,"control"),filepath.Join(dir,"capture"))
				if err:=rnValidate(t,cfg);err!=nil { t.Fatalf("configured default %q must support exclusive review evidence without requiring ledger replacement (keyed=%v): %v",project,keyed,err) }
				// Missing capability preserves ordinary bookkeeping, not review authority.
				ordinary:=rpClone(t,cfg);delete(ordinary,"review_producer")
				delete(ordinary["substrates"].([]any)[0].(map[string]any),"review_evidence_table")
				if err:=rnValidate(t,ordinary);err!=nil { t.Fatalf("ordinary native configuration regressed: %v",err) }
				for _, alias:=range []string{"Tasks","TaskLinks","TaskDeps","tAsKs"} {
					bad:=rpClone(t,cfg);bad["substrates"].([]any)[0].(map[string]any)["review_evidence_table"]=alias
					if err:=rnValidate(t,bad);err==nil || !strings.Contains(err.Error(),"review_evidence_table") { t.Fatalf("ordinary-table alias %q did not reach the exclusive evidence descriptor refusal: %v",alias,err) }
				}
				bad:=rpClone(t,cfg);bad["default_project_id"]=""
				if err:=rnValidate(t,bad);err==nil { t.Fatal("missing default project accepted") }
				bad=rpClone(t,cfg);bad["substrates"].([]any)[0].(map[string]any)["review_evidence_table"]="IndependentReviewEvidence;Tasks"
				if err:=rnValidate(t,bad);err==nil { t.Fatal("unsafe evidence table identifier accepted") }
			})
		}
	}
}
