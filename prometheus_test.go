package qlog

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestCollectorReportsIOFailure(t *testing.T) {
	failure := errors.New("disk failure")
	file := &testFile{writeFn: func([]byte) (int, error) { return 0, failure }}
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return file, nil })
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(w)); err != nil {
		t.Fatal(err)
	}
	checkFailed := func(want float64) {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			if family.GetName() == "qlog_failed" {
				if len(family.Metric) != 1 || family.Metric[0].GetGauge().GetValue() != want {
					t.Fatalf("qlog_failed: got %v, want %g", family, want)
				}
				return
			}
		}
		t.Fatal("missing qlog_failed metric")
	}
	checkFailed(0)
	if _, err := w.Write([]byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
	checkFailed(1)
}

func TestDashboardMetricsExist(t *testing.T) {
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return &testFile{}, nil })
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(w)); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, family := range families {
		names[family.GetName()] = true
	}
	data, err := os.ReadFile("monitoring/grafana-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Templating struct {
			List []struct {
				Name  string `json:"name"`
				Query string `json:"query"`
			} `json:"list"`
		} `json:"templating"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatal(err)
	}
	metric := regexp.MustCompile(`\bqlog_[a-zA-Z0-9_]+`)
	used := make(map[string]bool)
	check := func(t *testing.T, query string) {
		t.Helper()
		for _, name := range metric.FindAllString(query, -1) {
			used[name] = true
			if !names[name] {
				t.Errorf("query references unexported metric %s", name)
			}
		}
	}
	for _, variable := range dashboard.Templating.List {
		t.Run("variable/"+variable.Name, func(t *testing.T) { check(t, variable.Query) })
	}
	for _, panel := range dashboard.Panels {
		t.Run("panel/"+panel.Title, func(t *testing.T) {
			for _, target := range panel.Targets {
				check(t, target.Expr)
			}
		})
	}
	if !used["qlog_failed"] {
		t.Error("dashboard does not query unresolved I/O failures")
	}
}
