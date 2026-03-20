package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"time"

	aether "github.com/BabySid/aether"
	"github.com/BabySid/aether/model"
)

// ReportData is passed to the HTML template.
type ReportData struct {
	WorkflowPath string
	RunID        uint64
	StartTime    time.Time
	Elapsed      time.Duration
	Execution    *aether.WorkflowExecution
	Snapshots    []Snapshot
	RawWorkflow  []byte
}

// phaseClass maps a Phase to a CSS class for coloring.
func phaseClass(p model.Phase) string {
	switch p {
	case model.PhaseSucceeded:
		return "ph-succeeded"
	case model.PhaseFailed:
		return "ph-failed"
	case model.PhaseError:
		return "ph-error"
	case model.PhaseTimeout:
		return "ph-timeout"
	case model.PhaseCancelled:
		return "ph-cancelled"
	case model.PhaseSkipped:
		return "ph-skipped"
	case model.PhaseRunning:
		return "ph-running"
	case model.PhasePending:
		return "ph-pending"
	default:
		return "ph-unknown"
	}
}

// generateHTMLReport renders the HTML report string.
func generateHTMLReport(data ReportData) string {
	// Pretty-print workflow JSON
	var wfPretty bytes.Buffer
	if err := json.Indent(&wfPretty, data.RawWorkflow, "", "  "); err != nil {
		wfPretty.Write(data.RawWorkflow)
	}

	// Serialise snapshots to JSON strings for the embedded JS
	snapshotsJSON, _ := json.Marshal(data.Snapshots)

	funcMap := template.FuncMap{
		"phaseClass": phaseClass,
		"fmtTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05.000")
		},
		"fmtDuration": func(d time.Duration) string {
			return d.Round(time.Millisecond).String()
		},
		"add":       func(a, b int) int { return a + b },
		"taskCount": func(tasks []aether.TaskExecution) int { return len(tasks) },
		"terminalCount": func(tasks []aether.TaskExecution) int {
			n := 0
			for _, t := range tasks {
				if t.Phase.IsTerminal() {
					n++
				}
			}
			return n
		},
	}

	const tmplStr = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Aether Playground Report — {{.WorkflowPath}}</title>
<style>
  /* ---- Reset & Base ---- */
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #f4f6f9; color: #2d3748; line-height: 1.5; }
  h1 { font-size: 1.5rem; font-weight: 700; }
  h2 { font-size: 1.1rem; font-weight: 600; margin-bottom: .75rem; color: #4a5568; text-transform: uppercase;
       letter-spacing: .05em; }
  a { color: #3182ce; text-decoration: none; }

  /* ---- Layout ---- */
  .page { max-width: 1280px; margin: 0 auto; padding: 1.5rem; }
  .header { background: #1a202c; color: #fff; padding: 1.5rem 2rem; border-radius: .75rem;
             margin-bottom: 1.5rem; display: flex; justify-content: space-between; align-items: center; }
  .header h1 { font-size: 1.4rem; }
  .header .meta { font-size: .85rem; color: #a0aec0; margin-top: .3rem; }

  .grid { display: grid; grid-template-columns: 1fr 2fr; gap: 1.25rem; margin-bottom: 1.25rem; }
  .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 1.25rem; }
  @media (max-width: 768px) { .grid, .grid-3 { grid-template-columns: 1fr; } }

  .card { background: #fff; border-radius: .75rem; padding: 1.25rem 1.5rem;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  .card.full { grid-column: 1 / -1; }

  /* ---- Status Badge ---- */
  .badge { display: inline-block; padding: .2rem .65rem; border-radius: 9999px;
           font-size: .78rem; font-weight: 600; letter-spacing: .03em; }
  .ph-succeeded { background:#c6f6d5; color:#22543d; }
  .ph-failed    { background:#fed7d7; color:#742a2a; }
  .ph-error     { background:#feebc8; color:#7b341e; }
  .ph-timeout   { background:#fefcbf; color:#744210; }
  .ph-cancelled { background:#e9d8fd; color:#44337a; }
  .ph-skipped   { background:#e2e8f0; color:#4a5568; }
  .ph-running   { background:#bee3f8; color:#2a4365; }
  .ph-pending   { background:#e2e8f0; color:#718096; }
  .ph-unknown   { background:#e2e8f0; color:#718096; }

  /* ---- Stat Cards ---- */
  .stat-card { text-align: center; }
  .stat-value { font-size: 2rem; font-weight: 700; color: #2d3748; }
  .stat-label { font-size: .8rem; color: #718096; margin-top: .2rem; }

  /* ---- Tables ---- */
  table { width: 100%; border-collapse: collapse; font-size: .88rem; }
  th { text-align: left; padding: .55rem .75rem; background: #f7fafc;
       color: #718096; font-weight: 600; font-size: .78rem; text-transform: uppercase;
       letter-spacing: .05em; border-bottom: 1px solid #e2e8f0; }
  td { padding: .55rem .75rem; border-bottom: 1px solid #f0f4f8; vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f7fafc; }

  .path { font-family: "SFMono-Regular", Consolas, monospace; font-size: .8rem; color: #4a5568; }

  /* ---- Timeline ---- */
  .timeline { }
  .timeline-item { display: flex; gap: 1rem; padding: .6rem 0;
                   border-bottom: 1px solid #f0f4f8; align-items: flex-start; cursor: pointer; }
  .timeline-item:last-child { border-bottom: none; }
  .timeline-item:hover { background: #f7fafc; border-radius: .4rem; }
  .timeline-item.active .tl-seq { background: #3182ce; color: #fff; }
  .tl-seq { flex-shrink: 0; width: 2rem; height: 2rem; border-radius: 50%;
             background: #ebf4ff; color: #3182ce; font-size: .75rem; font-weight: 700;
             display: flex; align-items: center; justify-content: center; transition: background .15s; }
  .tl-op { font-weight: 600; font-size: .85rem; }
  .tl-time { font-size: .75rem; color: #a0aec0; }
  .tl-detail { font-size: .78rem; color: #718096; margin-top: .15rem; }

  /* ---- Code Block ---- */
  .code-block { background: #1a202c; color: #e2e8f0; border-radius: .5rem;
                padding: 1rem 1.25rem; overflow: auto; max-height: 24rem;
                font-family: "SFMono-Regular", Consolas, monospace; font-size: .8rem;
                line-height: 1.6; white-space: pre; }

  /* ---- Snapshot Explorer ---- */
  #snap-controls { display: flex; gap: .75rem; align-items: center; margin-bottom: .75rem; flex-wrap: wrap; }
  #snap-slider { flex: 1; min-width: 200px; }
  #snap-label { font-weight: 600; font-size: .9rem; min-width: 6rem; }
  #snap-prev, #snap-next { padding: .3rem .8rem; border: 1px solid #e2e8f0; border-radius: .4rem;
                            background: #fff; cursor: pointer; font-size: .85rem; }
  #snap-prev:hover, #snap-next:hover { background: #ebf4ff; }

  /* snap header */
  #snap-op-bar { font-size: .82rem; color: #718096; margin-bottom: .6rem; padding: .45rem .7rem;
                  background: #f7fafc; border-radius: .4rem; border-left: 3px solid #a0aec0; }

  /* diff legend */
  .diff-legend { display: flex; gap: 1rem; font-size: .72rem; color: #718096;
                  margin-bottom: .6rem; flex-wrap: wrap; }
  .diff-legend-item { display: flex; align-items: center; gap: .3rem; }
  .dl-swatch { width: .9rem; height: .9rem; border-radius: .2rem; flex-shrink: 0; }
  .dl-new     { background: #f0fff4; border: 2px solid #48bb78; }
  .dl-changed { background: #fce8d5; border: 2px solid #dd6b20; }
  .dl-same    { background: #f7fafc; border: 1px solid #e2e8f0; }

  /* snap detail tables */
  #snap-detail { overflow-y: auto; }
  .snap-section-title { font-size: .72rem; font-weight: 700; text-transform: uppercase;
                         letter-spacing: .06em; color: #a0aec0; margin: .5rem 0 .3rem; }
  #snap-detail table { font-size: .8rem; margin-bottom: .75rem; }
  #snap-detail th { font-size: .72rem; padding: .38rem .55rem; }
  #snap-detail td { padding: .38rem .55rem; }

  /* diff row / cell */
  tr.row-new td { background: #f0fff4 !important; }
  tr.row-new td:first-child { border-left: 3px solid #48bb78; }
  td.cell-changed { background: #fce8d5 !important; border-left: 3px solid #dd6b20; }

  /* ---- Progress Bar ---- */
  .progress-bar { height: .5rem; background: #e2e8f0; border-radius: 9999px; overflow: hidden; }
  .progress-fill { height: 100%; background: #48bb78; border-radius: 9999px;
                   transition: width .3s ease; }
</style>
</head>
<body>
<div class="page">

  <!-- Header -->
  <div class="header">
    <div>
      <h1>⚙️ Aether Playground Report</h1>
      <div class="meta">{{.WorkflowPath}} &nbsp;·&nbsp; Run ID: {{.RunID}}</div>
    </div>
    <div style="text-align:right">
      <span class="badge {{phaseClass .Execution.Phase}}" style="font-size:.95rem;padding:.35rem 1rem">
        {{.Execution.Phase}}
      </span>
      <div class="meta" style="margin-top:.4rem">{{fmtTime .StartTime}}</div>
    </div>
  </div>

  <!-- Stats -->
  <div class="grid-3">
    <div class="card stat-card">
      <div class="stat-value">{{fmtDuration .Elapsed}}</div>
      <div class="stat-label">Total Elapsed</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value">{{taskCount .Execution.Tasks}}</div>
      <div class="stat-label">Total Tasks</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value">{{terminalCount .Execution.Tasks}} / {{taskCount .Execution.Tasks}}</div>
      <div class="stat-label">Completed / Total</div>
    </div>
  </div>

  <!-- Progress -->
  {{if .Execution.Progress}}
  <div class="card" style="margin-bottom:1.25rem;padding:.9rem 1.5rem">
    <div style="display:flex;justify-content:space-between;margin-bottom:.4rem">
      <span style="font-size:.85rem;font-weight:600;color:#4a5568">Progress</span>
      <span style="font-size:.85rem;color:#718096">{{.Execution.Progress}}</span>
    </div>
    <div class="progress-bar">
      <div class="progress-fill" id="progress-fill"></div>
    </div>
  </div>
  {{end}}

  <!-- Tasks Table -->
  <div class="card" style="margin-bottom:1.25rem">
    <h2>Task Executions</h2>
    <table>
      <thead>
        <tr>
          <th>#</th><th>Task Name</th><th>Path</th><th>Template</th><th>Phase</th>
          <th>Started</th><th>Finished</th><th>Duration</th>
        </tr>
      </thead>
      <tbody>
        {{range $i, $t := .Execution.Tasks}}
        <tr>
          <td style="color:#a0aec0">{{add $i 1}}</td>
          <td><strong>{{$t.Name}}</strong></td>
          <td><span class="path">{{$t.Path}}</span></td>
          <td><span class="path">{{$t.Template}}</span></td>
          <td><span class="badge {{phaseClass $t.Phase}}">{{$t.Phase}}</span></td>
          <td style="font-size:.8rem;color:#718096">
            {{if $t.Metrics}}{{$t.Metrics.StartedAt}}{{end}}
          </td>
          <td style="font-size:.8rem;color:#718096">
            {{if $t.Metrics}}{{$t.Metrics.FinishedAt}}{{end}}
          </td>
          <td style="font-size:.8rem;color:#718096">
            {{if $t.Metrics}}{{$t.Metrics.Duration}}{{end}}
          </td>
        </tr>
        {{end}}
      </tbody>
    </table>
  </div>

  <!-- Timeline & Snapshot Explorer -->
  <div class="grid">
    <!-- Store Change Timeline -->
    <div class="card" style="overflow-y:auto;max-height:60vh">
      <h2>Store Change Timeline</h2>
      <div class="timeline" id="tl-root">
        {{range .Snapshots}}
        <div class="timeline-item" id="tl-{{.Seq}}" onclick="snapRender({{add .Seq -1}})">
          <div class="tl-seq">{{.Seq}}</div>
          <div>
            <div class="tl-op">{{.Operation}}
              <span style="font-size:.75rem;font-weight:400;color:#718096">(entity {{.EntityID}})</span>
            </div>
            <div class="tl-time">{{fmtTime .Time}}</div>
            <div class="tl-detail">
              {{len .TaskRuns}} task run(s) &nbsp;·&nbsp; {{len .WorkflowRuns}} workflow run(s)
            </div>
          </div>
        </div>
        {{end}}
      </div>
    </div>

    <!-- Snapshot Explorer -->
    <div class="card">
      <h2>Snapshot Explorer</h2>
      <div id="snap-controls">
        <button id="snap-prev" onclick="snapMove(-1)">◀</button>
        <input id="snap-slider" type="range" min="0" max="{{len .Snapshots | add -1}}"
               value="0" oninput="snapRender(parseInt(this.value))">
        <button id="snap-next" onclick="snapMove(1)">▶</button>
        <span id="snap-label">Step 1 / {{len .Snapshots}}</span>
      </div>

      <!-- diff legend -->
      <div class="diff-legend">
        <div class="diff-legend-item"><div class="dl-swatch dl-new"></div><span>新增记录</span></div>
        <div class="diff-legend-item"><div class="dl-swatch dl-changed"></div><span>字段变更</span></div>
        <div class="diff-legend-item"><div class="dl-swatch dl-same"></div><span>无变化</span></div>
      </div>

      <!-- operation summary bar -->
      <div id="snap-op-bar" style="display:none"></div>

      <!-- data tables rendered here -->
      <div id="snap-detail">
        <em style="color:#a0aec0;font-size:.85rem">Move the slider to inspect each store state.</em>
      </div>
    </div>
  </div>

  <!-- Workflow JSON -->
  <div class="card full">
    <h2>Workflow Definition</h2>
    <div class="code-block">{{.RawWorkflow}}</div>
  </div>

</div><!-- /page -->

<script>
const SNAPS = {{.SnapshotsJSON}};
const TOTAL = SNAPS.length;
let current = 0;

/* ---- helpers ---- */
function phaseClass(p) {
  const m = {
    Succeeded:'ph-succeeded', Failed:'ph-failed', Error:'ph-error',
    Timeout:'ph-timeout', Cancelled:'ph-cancelled', Skipped:'ph-skipped',
    Running:'ph-running', Pending:'ph-pending'
  };
  return m[p] || 'ph-unknown';
}
function badge(p) {
  return '<span class="badge ' + phaseClass(p) + '">' + (p || '—') + '</span>';
}
function esc(v) {
  if (v == null || v === '') return '<span style="color:#cbd5e0">—</span>';
  return String(v)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

/* ---- diff helpers ---- */
function indexBy(arr, key) {
  const m = {};
  if (!arr) return m;
  arr.forEach(function(r) { m[r[key]] = r; });
  return m;
}
function fieldChanged(prev, curr, field) {
  return prev && JSON.stringify(prev[field]) !== JSON.stringify(curr[field]);
}
function cell(prev, curr, field, display) {
  var cls = fieldChanged(prev, curr, field) ? ' class="cell-changed"' : '';
  var content = (display !== undefined) ? display : esc(curr[field]);
  return '<td' + cls + '>' + content + '</td>';
}

/* ---- render workflow-runs table ---- */
function renderWfTable(snap, prevWfIdx) {
  if (!snap.workflowRuns || snap.workflowRuns.length === 0) return '';
  var html = '<div class="snap-section-title">Workflow Runs (' + snap.workflowRuns.length + ')</div>'
    + '<table><thead><tr>'
    + '<th>Run ID</th><th>Status</th><th>Message</th><th>Token</th><th>Updated At</th>'
    + '</tr></thead><tbody>';
  snap.workflowRuns.forEach(function(r) {
    var prev = prevWfIdx ? prevWfIdx[r.runID] : null;
    var rowCls = !prev ? ' class="row-new"' : '';
    html += '<tr' + rowCls + '>'
      + '<td><span style="font-family:monospace;font-size:.75rem;color:#a0aec0">' + r.runID + '</span></td>'
      + cell(prev, r, 'status',    badge(r.status))
      + cell(prev, r, 'message',   esc(r.message))
      + cell(prev, r, 'token',     '<span style="font-size:.75rem;color:#a0aec0">' + r.token + '</span>')
      + cell(prev, r, 'updatedAt', '<span style="font-size:.75rem;color:#718096">' + esc(r.updatedAt) + '</span>')
      + '</tr>';
  });
  html += '</tbody></table>';
  return html;
}

/* ---- render task-runs table ---- */
function renderTaskTable(snap, prevTaskIdx) {
  if (!snap.taskRuns || snap.taskRuns.length === 0) return '';
  var html = '<div class="snap-section-title">Task Runs (' + snap.taskRuns.length + ')</div>'
    + '<table><thead><tr>'
    + '<th>Run ID</th><th>Task Name</th><th>Scope</th><th>Status</th>'
    + '<th>Message</th><th>Retries</th><th>Token</th><th>Updated At</th>'
    + '</tr></thead><tbody>';
  snap.taskRuns.forEach(function(r) {
    var prev = prevTaskIdx ? prevTaskIdx[r.runID] : null;
    var rowCls = !prev ? ' class="row-new"' : '';
    html += '<tr' + rowCls + '>'
      + '<td><span style="font-family:monospace;font-size:.75rem;color:#a0aec0">' + r.runID + '</span></td>'
      + cell(prev, r, 'taskName',   '<strong>' + esc(r.taskName) + '</strong>')
      + cell(prev, r, 'scope',      '<span class="path">' + esc(r.scope || '/') + '</span>')
      + cell(prev, r, 'status',     badge(r.status))
      + cell(prev, r, 'message',    esc(r.message))
      + cell(prev, r, 'retryCount', '<span style="display:block;text-align:center">' + (r.retryCount||0) + '</span>')
      + cell(prev, r, 'token',      '<span style="font-size:.75rem;color:#a0aec0">' + r.token + '</span>')
      + cell(prev, r, 'updatedAt',  '<span style="font-size:.75rem;color:#718096">' + esc(r.updatedAt) + '</span>')
      + '</tr>';
  });
  html += '</tbody></table>';
  return html;
}

/* ---- main render ---- */
function snapRender(idx) {
  if (!SNAPS || SNAPS.length === 0) return;
  idx = Math.max(0, Math.min(idx, TOTAL - 1));
  current = idx;

  // sync slider + label
  document.getElementById('snap-slider').value = idx;
  document.getElementById('snap-label').textContent = 'Step ' + (idx+1) + ' / ' + TOTAL;

  // highlight active timeline item
  document.querySelectorAll('.timeline-item').forEach(function(el) {
    el.classList.remove('active');
  });
  var tlItem = document.getElementById('tl-' + (idx+1));
  if (tlItem) {
    tlItem.classList.add('active');
    tlItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }

  // build previous-snapshot indexes for diff
  var prevWfIdx   = idx > 0 ? indexBy(SNAPS[idx-1].workflowRuns, 'runID') : null;
  var prevTaskIdx = idx > 0 ? indexBy(SNAPS[idx-1].taskRuns,     'runID') : null;

  var snap = SNAPS[idx];

  // operation bar
  var opBar = document.getElementById('snap-op-bar');
  var opColor = snap.operation.indexOf('Create') !== -1 ? '#38a169'
              : snap.operation.indexOf('Update') !== -1 ? '#dd6b20' : '#3182ce';
  opBar.style.borderLeftColor = opColor;
  opBar.style.display = 'block';
  opBar.innerHTML = '<strong style="color:' + opColor + '">' + esc(snap.operation) + '</strong>'
    + ' &nbsp;·&nbsp; entity ' + snap.entityID
    + ' &nbsp;·&nbsp; <span style="color:#a0aec0">' + esc(snap.time) + '</span>';

  // tables
  document.getElementById('snap-detail').innerHTML =
    renderWfTable(snap, prevWfIdx) + renderTaskTable(snap, prevTaskIdx);
}

function snapMove(delta) { snapRender(current + delta); }

/* ---- Progress bar ---- */
(function() {
  var fillEl = document.getElementById('progress-fill');
  if (!fillEl) return;
  var prog = {{.Execution.Progress | printf "%q"}};
  var parts = prog.replace(/"/g,'').split('/');
  if (parts.length === 2) {
    var pct = Math.round(parseInt(parts[0]) / parseInt(parts[1]) * 100);
    fillEl.style.width = pct + '%';
  }
})();

// Auto-render first snapshot
if (TOTAL > 0) snapRender(0);
</script>
</body>
</html>`

	type tplData struct {
		WorkflowPath  string
		RunID         uint64
		StartTime     time.Time
		Elapsed       time.Duration
		Execution     *aether.WorkflowExecution
		Snapshots     []Snapshot
		RawWorkflow   template.HTML
		SnapshotsJSON template.JS
	}

	tmpl, err := template.New("report").Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return fmt.Sprintf("<!-- template parse error: %v -->", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, tplData{
		WorkflowPath:  data.WorkflowPath,
		RunID:         data.RunID,
		StartTime:     data.StartTime,
		Elapsed:       data.Elapsed,
		Execution:     data.Execution,
		Snapshots:     data.Snapshots,
		RawWorkflow:   template.HTML(template.HTMLEscapeString(wfPretty.String())),
		SnapshotsJSON: template.JS(snapshotsJSON),
	}); err != nil {
		return fmt.Sprintf("<!-- template execute error: %v -->", err)
	}
	return buf.String()
}
