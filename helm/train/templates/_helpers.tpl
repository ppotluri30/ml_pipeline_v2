{{- define "train.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "train.fullname" -}}
{{- printf "%s-%s" (include "train.name" .) .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}
