{{- define "prep.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "prep.fullname" -}}
{{- printf "%s-%s" (include "prep.name" .) .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}
