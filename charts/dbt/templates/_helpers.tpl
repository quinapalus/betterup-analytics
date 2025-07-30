{{- define "dbt.fullname" -}}
{{- .Release.Name }}
{{- end }}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dbt.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dbt.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dbt.labels" -}}
helm.sh/chart: {{ include "dbt.chart" . }}
{{ include "dbt.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "dbt.envFromSpec" -}}
      - secretRef:
          name: {{ include "dbt.fullname" . }}
      - configMapRef:
          name: {{ include "dbt.fullname" . }}
{{- end }}

{{- define "dbt.jobSpec" -}}
backoffLimit: 0
template:
  spec:
{{- if .Values.dbt.seed.enabled }}
    initContainers:
    - name: dbt-seed
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
      imagePullPolicy: IfNotPresent
      command:
      - dbt
      - seed
      args: {{ .Values.dbt.seed.args | toJson }}
      envFrom: 
      {{ include "dbt.envFromSpec" . }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
{{- end }}
    containers:
    - name: dbt-run
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
      imagePullPolicy: IfNotPresent
      command: {{ .Values.dbt.run.command | toJson}}
      args: {{ .Values.dbt.run.args | toJson }}
      envFrom: 
      {{ include "dbt.envFromSpec" . }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
    restartPolicy: Never
{{- end }}
