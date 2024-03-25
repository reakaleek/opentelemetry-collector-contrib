package githubactionslogreceiver

import (
	"archive/zip"
	"github.com/google/go-github/v60/github"
	"sort"
	"time"
)

type GitHubWorkflowRunWebhookPayload struct {
	Action      string             `json:"action"`
	Repository  github.Repository  `json:"repository"`
	WorkflowRun github.WorkflowRun `json:"workflow_run"`
	Workflow    github.Workflow    `json:"workflow"`
}

type Status string
type Conclusion string

type Repository struct {
	FullName string
}

type Run struct {
	Repository Repository
	Workflow   Workflow
	Jobs       []Job
	RunAttempt int64
}

type Workflow struct {
	Name string
}

type Job struct {
	ID          int64
	Status      Status
	Conclusion  Conclusion
	Name        string
	Steps       Steps
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	URL         string    `json:"html_url"`
	RunID       int64     `json:"run_id"`
}

type Step struct {
	Name        string
	Status      Status
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Conclusion  Conclusion
	Number      int
	Log         *zip.File
}

type Steps []Step

func (s Steps) Len() int           { return len(s) }
func (s Steps) Less(i, j int) bool { return s[i].Number < s[j].Number }
func (s Steps) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Steps) Sort() {
	sort.Sort(s)
}
