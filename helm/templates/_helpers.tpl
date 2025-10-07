{{/*
Expand the name of the chart.
*/}}
{{- define "data-processing-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "data-processing-pipeline.fullname" -}}
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
{{- define "data-processing-pipeline.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "data-processing-pipeline.labels" -}}
helm.sh/chart: {{ include "data-processing-pipeline.chart" . }}
{{ include "data-processing-pipeline.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "data-processing-pipeline.selectorLabels" -}}
app.kubernetes.io/name: {{ include "data-processing-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service labels
*/}}
{{- define "data-processing-pipeline.serviceLabels" -}}
{{ include "data-processing-pipeline.labels" . }}
app.kubernetes.io/component: service
{{- end }}

{{/*
Service selector labels
*/}}
{{- define "data-processing-pipeline.serviceSelectorLabels" -}}
{{ include "data-processing-pipeline.selectorLabels" . }}
app.kubernetes.io/component: service
{{- end }}

{{/*
Service name
*/}}
{{- define "data-processing-pipeline.serviceName" -}}
{{- printf "%s-%s" (include "data-processing-pipeline.fullname" .) .serviceName }}
{{- end }}

{{/*
Service full name
*/}}
{{- define "data-processing-pipeline.serviceFullName" -}}
{{- printf "%s-%s" (include "data-processing-pipeline.fullname" .) .serviceName }}
{{- end }}

{{/*
Image name
*/}}
{{- define "data-processing-pipeline.image" -}}
{{- $registry := .global.imageRegistry | default .Values.global.imageRegistry }}
{{- if $registry }}
{{- printf "%s/%s:%s" $registry .image.repository .image.tag }}
{{- else }}
{{- printf "%s:%s" .image.repository .image.tag }}
{{- end }}
{{- end }}

{{/*
Environment variables
*/}}
{{- define "data-processing-pipeline.env" -}}
{{- range $key, $value := .env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}

{{/*
Security context
*/}}
{{- define "data-processing-pipeline.securityContext" -}}
{{- if .Values.security.enabled }}
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
{{- end }}
{{- end }}

{{/*
Pod security context
*/}}
{{- define "data-processing-pipeline.podSecurityContext" -}}
{{- if .Values.security.enabled }}
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
{{- end }}
{{- end }}

{{/*
mTLS volume mounts
*/}}
{{- define "data-processing-pipeline.mtlsVolumeMounts" -}}
{{- if .Values.security.mtls.enabled }}
- name: mtls-certs
  mountPath: {{ .Values.security.mtls.certDir }}
  readOnly: true
{{- end }}
{{- end }}

{{/*
mTLS volumes
*/}}
{{- define "data-processing-pipeline.mtlsVolumes" -}}
{{- if .Values.security.mtls.enabled }}
- name: mtls-certs
  secret:
    secretName: {{ include "data-processing-pipeline.fullname" . }}-mtls-certs
{{- end }}
{{- end }}

{{/*
Resource limits
*/}}
{{- define "data-processing-pipeline.resources" -}}
{{- if .resources }}
resources:
  {{- if .resources.limits }}
  limits:
    {{- if .resources.limits.cpu }}
    cpu: {{ .resources.limits.cpu }}
    {{- end }}
    {{- if .resources.limits.memory }}
    memory: {{ .resources.limits.memory }}
    {{- end }}
  {{- end }}
  {{- if .resources.requests }}
  requests:
    {{- if .resources.requests.cpu }}
    cpu: {{ .resources.requests.cpu }}
    {{- end }}
    {{- if .resources.requests.memory }}
    memory: {{ .resources.requests.memory }}
    {{- end }}
  {{- end }}
{{- end }}
{{- end }}

{{/*
Health check probes
*/}}
{{- define "data-processing-pipeline.probes" -}}
livenessProbe:
  httpGet:
    path: /health/live
    port: {{ .service.port }}
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /health/ready
    port: {{ .service.port }}
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
{{- end }}

{{/*
Network policy
*/}}
{{- define "data-processing-pipeline.networkPolicy" -}}
{{- if .Values.security.networkPolicies.enabled }}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "data-processing-pipeline.serviceName" . }}
  labels:
    {{- include "data-processing-pipeline.serviceLabels" . | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      {{- include "data-processing-pipeline.serviceSelectorLabels" . | nindent 6 }}
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: {{ .Release.Namespace }}
    ports:
    - protocol: TCP
      port: {{ .service.port }}
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: {{ .Release.Namespace }}
    ports:
    - protocol: TCP
      port: 9092  # Kafka
  - to:
    - namespaceSelector:
        matchLabels:
          name: {{ .Release.Namespace }}
    ports:
    - protocol: TCP
      port: 8123  # ClickHouse
  - to:
    - namespaceSelector:
        matchLabels:
          name: {{ .Release.Namespace }}
    ports:
    - protocol: TCP
      port: 5432  # PostgreSQL
  - to:
    - namespaceSelector:
        matchLabels:
          name: {{ .Release.Namespace }}
    ports:
    - protocol: TCP
      port: 6379  # Redis
{{- end }}
{{- end }}
