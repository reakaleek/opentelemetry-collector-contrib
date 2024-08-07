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
	SeverityText   string
}

type Repository struct {
	FullName string
	Org      string
	Name     string
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
	Event        string
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ActorLogin   string
	ActorID      int64 `json:"actor_id"`
	HeadBranch   string
}

type Job struct {
	ID              int64
	Status          string
	Conclusion      string
	Name            string
	Steps           Steps
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`
	URL             string    `json:"html_url"`
	RunID           int64     `json:"run_id"`
	RunnerName      string    `json:"runner_name"`
	RunnerGroupName string    `json:"runner_group_name"`
	RunnerGroupID   int64     `json:"runner_group_id"`
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
	for i, s := range job.Steps {
		steps[i] = Step{
			Name:        s.GetName(),
			Status:      s.GetStatus(),
			Conclusion:  s.GetConclusion(),
			Number:      s.GetNumber(),
			StartedAt:   s.GetStartedAt().Time,
			CompletedAt: s.GetCompletedAt().Time,
		}
	}
	return Job{
		ID:              job.GetID(),
		Status:          job.GetStatus(),
		Conclusion:      job.GetConclusion(),
		Name:            job.GetName(),
		StartedAt:       job.GetStartedAt().Time,
		CompletedAt:     job.GetCompletedAt().Time,
		URL:             job.GetHTMLURL(),
		RunID:           job.GetRunID(),
		Steps:           steps,
		RunnerName:      job.GetRunnerName(),
		RunnerGroupName: job.GetRunnerGroupName(),
		RunnerGroupID:   job.GetRunnerGroupID(),
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
		Event:        run.GetEvent(),
		CreatedAt:    run.GetCreatedAt().Time,
		UpdatedAt:    run.GetUpdatedAt().Time,
		ActorLogin:   run.GetActor().GetLogin(),
		ActorID:      run.GetActor().GetID(),
		HeadBranch:   run.GetHeadBranch(),
	}
}

func mapRepository(repo *github.Repository) Repository {
	return Repository{
		FullName: repo.GetFullName(),
		Org:      repo.GetOwner().GetLogin(),
		Name:     repo.GetName(),
	}
}
