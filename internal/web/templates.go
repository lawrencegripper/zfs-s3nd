package web

import (
	"database/sql"
	"embed"
	"fmt"
	"html/template"
	"strings"
)

//go:embed templates/*.html templates/admin.css templates/admin.js
var pageTemplateFS embed.FS

func mustReadEmbeddedText(filename string) string {
	data, err := pageTemplateFS.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func mustPageTemplate(filename string) *template.Template {
	return template.Must(template.New(filename).Funcs(webTemplateFuncs()).ParseFS(pageTemplateFS, "templates/"+filename))
}

func webTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"pageCSS": func() template.CSS { return adminPageCSS },
		"pageJS":  func() template.JS { return adminPageJS },
		"copyButton": func(targetID string) template.HTML {
			targetID = template.HTMLEscapeString(targetID)
			return template.HTML(`<button type="button" class="copy-button" data-copy-target="` + targetID + `" aria-label="Copy command"><svg aria-hidden="true" viewBox="0 0 24 24"><path d="M8 7V5a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2h-2v1a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h3Zm2 0h5a2 2 0 0 1 2 2v7h2V5h-9v2Zm5 2H5v10h10V9Z"/></svg><span>Copy</span></button>`)
		},
		"nav": func(csrfToken string) template.HTML {
			return template.HTML(adminNavHTML(csrfToken))
		},
		"add":      func(a, b int) int { return a + b },
		"time":     formatDisplayTime,
		"label":    humanLabel,
		"badge":    statusBadgeClass,
		"badgeAny": func(v interface{}) string { return countBadgeClass(int64Value(v)) },
		"nullString": func(v sql.NullString) string {
			if !v.Valid {
				return ""
			}
			return v.String
		},
		"bytes": func(v int64) string {
			return formatBytes(v)
		},
		"bytesAny": func(v interface{}) string {
			return formatBytes(int64Value(v))
		},
		"nullBytes": formatNullBytes,
		"value": func(v interface{}) string {
			if v == nil {
				return ""
			}
			return fmt.Sprint(v)
		},
	}
}

var adminPageCSS = template.CSS(mustReadEmbeddedText("templates/admin.css"))
var adminPageJS = template.JS(mustReadEmbeddedText("templates/admin.js"))

func adminNavHTML(csrfToken string) string {
	escapedToken := template.HTMLEscapeString(csrfToken)
	return strings.Join([]string{
		`<nav class="topbar" aria-label="Primary"><div class="topbar-inner">`,
		`<a class="brand" href="/">ZFS S3nd</a>`,
		`<a class="nav-link" href="/">Dashboard</a>`,
		`<a class="nav-link" href="/datasets">Datasets</a>`,
		`<a class="nav-link" href="/activity">Activity</a>`,
		`<a class="nav-link" href="/status">Status</a>`,
		`<a class="nav-link" href="/settings">Settings</a>`,
		`<form class="logout" method="post" action="/logout">`,
		`<input type="hidden" name="csrf_token" value="`, escapedToken, `">`,
		`<button type="submit">Sign out</button>`,
		`</form></div></nav>`,
	}, "")
}

var (
	loginTemplate           = mustPageTemplate("login.html")
	setupTemplate           = mustPageTemplate("setup.html")
	apiTokenCreatedTemplate = mustPageTemplate("apiTokenCreated.html")
	dashboardTemplate       = mustPageTemplate("dashboard.html")
	activityTemplate        = mustPageTemplate("activity.html")
	statusTemplate          = mustPageTemplate("status.html")
	settingsTemplate        = mustPageTemplate("settings.html")
	datasetsTemplate        = mustPageTemplate("datasets.html")
	datasetDetailTemplate   = mustPageTemplate("datasetDetail.html")
	snapshotDetailTemplate  = mustPageTemplate("snapshotDetail.html")
)
