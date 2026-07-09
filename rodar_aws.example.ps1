# =============================================================================
# MODELO - copie para rodar_aws.ps1 e preencha com suas credenciais.
# O rodar_aws.ps1 esta no .gitignore e nao deve ser commitado.
# Rode:  powershell -ExecutionPolicy Bypass -File .\rodar_aws.ps1
# =============================================================================

$env:LAKE_URI              = "s3a://SEU-BUCKET/datalake"
$env:AWS_ACCESS_KEY_ID     = "COLE_AQUI"
$env:AWS_SECRET_ACCESS_KEY = "COLE_AQUI"
# Descomente se usar credenciais temporarias (AWS Academy / Learner Lab):
# $env:AWS_SESSION_TOKEN   = "COLE_AQUI"
$env:AWS_REGION            = "us-east-1"
$env:PYTHONIOENCODING      = "utf-8"

& python "$PSScriptRoot\pipeline_medalhao.py" batch
