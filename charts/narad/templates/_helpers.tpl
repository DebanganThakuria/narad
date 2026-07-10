{{/*
Expand the chart name.
*/}}
{{- define "narad.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "narad.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "narad.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "narad.labels" -}}
helm.sh/chart: {{ include "narad.chart" . }}
{{ include "narad.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Immutable labels: for StatefulSet volumeClaimTemplates, whose spec can
never change after creation. Deliberately excludes commonLabels — an
operator label added later must not brick every future upgrade.
*/}}
{{- define "narad.immutableLabels" -}}
helm.sh/chart: {{ include "narad.chart" . }}
{{ include "narad.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "narad.selectorLabels" -}}
app.kubernetes.io/name: {{ include "narad.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}


{{/*
Service account name.
*/}}
{{- define "narad.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "narad.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Headless service name for StatefulSet pod DNS.
*/}}
{{- define "narad.headlessServiceName" -}}
{{- printf "%s-headless" (include "narad.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The static three-voter peer list Narad expects.
*/}}
{{- define "narad.clusterPeers" -}}
{{- $fullname := include "narad.fullname" . -}}
{{- $headless := include "narad.headlessServiceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $domain := .Values.clusterDomain -}}
{{- $port := int .Values.service.ports.cluster -}}
{{- $replicas := int .Values.replicaCount -}}
{{- range $i := until $replicas -}}
{{- if $i }},{{ end -}}
{{- printf "%s-%d@%s-%d.%s.%s.svc.%s:%d" $fullname $i $fullname $i $headless $namespace $domain $port -}}
{{- end -}}
{{- end -}}

{{- define "narad.initialMembers" -}}
{{- $fullname := include "narad.fullname" . -}}
{{- $n := int (default 3 .Values.initialClusterSize) -}}
{{- range $i := until $n -}}
{{- if $i }},{{ end -}}
{{- printf "%s-%d" $fullname $i -}}
{{- end -}}
{{- end -}}

{{/*
Container image reference.
*/}}
{{- define "narad.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
