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
Number of pods in the pinned peer list: clusterPeerCount, or
initialClusterSize when unset (0).
*/}}
{{- define "narad.clusterPeerCount" -}}
{{- $n := int (default 0 .Values.clusterPeerCount) -}}
{{- if le $n 0 -}}
{{- $n = int (default 3 .Values.initialClusterSize) -}}
{{- end -}}
{{- $n -}}
{{- end -}}

{{/*
The PINNED Raft peer list (NARAD_CLUSTER_PEERS). Deliberately not derived
from replicaCount: it lives in the pod template, and a peer list that
changes with every scale operation rolls every existing member at the
same time as the join or the decommission. Joining pods walk this list to
find the leader; every pod advertises its own address through
NARAD_CLUSTER_ADVERTISE_ADDR, so pods beyond the list are fine.
*/}}
{{- define "narad.clusterPeers" -}}
{{- $fullname := include "narad.fullname" . -}}
{{- $headless := include "narad.headlessServiceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $domain := .Values.clusterDomain -}}
{{- $port := int .Values.service.ports.cluster -}}
{{- $n := int (include "narad.clusterPeerCount" .) -}}
{{- range $i := until $n -}}
{{- if $i }},{{ end -}}
{{- printf "%s-%d@%s-%d.%s.%s.svc.%s:%d" $fullname $i $fullname $i $headless $namespace $domain $port -}}
{{- end -}}
{{- end -}}

{{/*
This pod's own advertised Raft address: its stable headless DNS name.
$(POD_NAME) is expanded by the kubelet (POD_NAME is declared earlier in
the env list).
*/}}
{{- define "narad.clusterAdvertiseAddr" -}}
{{- printf "$(POD_NAME).%s.%s.svc.%s:%d" (include "narad.headlessServiceName" .) .Release.Namespace .Values.clusterDomain (int .Values.service.ports.cluster) -}}
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
