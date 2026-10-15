{{/*
Expand the name of the chart.
*/}}
{{- define "rag-chunk-lifecycle-service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "rag-chunk-lifecycle-service.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "rag-chunk-lifecycle-service.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "rag-chunk-lifecycle-service.labels" -}}
helm.sh/chart: {{ include "rag-chunk-lifecycle-service.chart" . }}
{{ include "rag-chunk-lifecycle-service.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "rag-chunk-lifecycle-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-chunk-lifecycle-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Chunk Server selector labels
*/}}
{{- define "rag-chunk-lifecycle-service.chunkServerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-chunk-lifecycle-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: chunk-server
{{- end }}

{{/*
Reconciliation Worker selector labels
*/}}
{{- define "rag-chunk-lifecycle-service.reconciliationWorkerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-chunk-lifecycle-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: reconciliation-worker
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "rag-chunk-lifecycle-service.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rag-chunk-lifecycle-service.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
