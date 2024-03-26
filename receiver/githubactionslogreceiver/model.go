package githubactionslogreceiver

import (
	"archive/zip"
	"github.com/google/go-github/v60/github"
	"sort"
	"time"
)

type LogLine struct {
	Body           string
	Timestamp      time.Time
	SeverityNumber int
}

type GitHubWorkflowRunEvent struct {
	Action      string
	Repository  github.Repository
	WorkflowRun github.WorkflowRun `json:"workflow_run"`
	Workflow    github.Workflow
}

type Repository struct {
	FullName string
}

type Run struct {
	ID           int64
	Name         string
	RunAttempt   int64     `json:"run_attempt"`
	RunNumber    int64     `json:"run_number"`
	RunStartedAt time.Time `json:"run_started_at"`
	URL          string    `json:"html_url"`
	Status       string
	Conclusion   string
}

type Job struct {
	ID          int64
	Status      string
	Conclusion  string
	Name        string
	Steps       Steps
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	URL         string    `json:"html_url"`
	RunID       int64     `json:"run_id"`
}

type Step struct {
	Name        string
	Status      string
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Conclusion  string
	Number      int64
	Log         *zip.File
}

type Steps []Step

func (s Steps) Len() int           { return len(s) }
func (s Steps) Less(i, j int) bool { return s[i].Number < s[j].Number }
func (s Steps) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Steps) Sort() {
	sort.Sort(s)
}

func mapJobs(workflowJobs []*github.WorkflowJob) []Job {
	result := make([]Job, len(workflowJobs))
	for i, job := range workflowJobs {
		result[i] = mapJob(job)
	}
	return result
}

func mapJob(job *github.WorkflowJob) Job {
	steps := make([]Step, len(job.Steps))
	for _, s := range job.Steps {
		steps = append(steps, Step{
			Name:        *s.Name,
			Status:      *s.Status,
			Conclusion:  *s.Conclusion,
			Number:      *s.Number,
			StartedAt:   s.GetStartedAt().Time,
			CompletedAt: s.GetCompletedAt().Time,
		})
	}
	Steps(steps).Sort()
	return Job{
		ID:          job.GetID(),
		Status:      job.GetStatus(),
		Conclusion:  job.GetConclusion(),
		Name:        job.GetName(),
		StartedAt:   job.GetStartedAt().Time,
		CompletedAt: job.GetCompletedAt().Time,
		URL:         job.GetHTMLURL(),
		RunID:       job.GetRunID(),
		Steps:       steps,
	}
}

func mapRun(run *github.WorkflowRun) Run {
	return Run{
		ID:           run.GetID(),
		Name:         run.GetName(),
		RunAttempt:   int64(run.GetRunAttempt()),
		RunNumber:    int64(run.GetRunNumber()),
		URL:          run.GetHTMLURL(),
		Status:       run.GetStatus(),
		Conclusion:   run.GetConclusion(),
		RunStartedAt: run.GetRunStartedAt().Time,
	}
}

func mapRepository(repo *github.Repository) Repository {
	return Repository{
		FullName: repo.GetFullName(),
	}
}
