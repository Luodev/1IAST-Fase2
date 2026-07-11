#!/usr/bin/env bash
# =============================================================================
# verificar_limpeza.sh — Audita se o cleanup.sh removeu todos os recursos AWS
# Tech Challenge Fase 2 — POSTECH AI Scientist
#
# Percorre cada recurso provisionado pelo setup.sh e confirma que foi
# removido. Idempotente e somente leitura: pode ser executado quantas vezes
# for necessário, sem risco de alterar o ambiente.
#
# Uso: bash verificar_limpeza.sh
# Saída: ✅ recurso removido | ❌ recurso remanescente (gera custo potencial)
# Código de saída: 0 = ambiente limpo | 1 = há recursos pendentes
#
# Padrão adotado do lab FIAP (verificar_limpeza.sh — fiap_pipe_kafka).
# =============================================================================

export AWS_REGION=us-east-1
export PROJECT=alfabetizacao-br
export ENV=dev

echo "============================================================"
echo "  Verificação de Limpeza — $PROJECT"
echo "  Região: $AWS_REGION"
echo "============================================================"
echo ""

CLEAN=true

# -----------------------------------------------------------------------------
# 1. MSK Cluster — recurso mais caro (~US$0,15/h); prioridade máxima
# -----------------------------------------------------------------------------
echo "=== MSK Cluster ==="
MSK=$(aws kafka list-clusters --region $AWS_REGION \
  --query "ClusterInfoList[?ClusterName=='${PROJECT}-msk'].{Name:ClusterName,State:State}" \
  --output text 2>/dev/null)
if [ -z "$MSK" ] || [ "$MSK" == "None" ]; then
  echo "  ✅ Cluster MSK: deletado"
else
  echo "  ❌ Cluster MSK: $MSK (GERANDO CUSTO — remova imediatamente)"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# 2. Buckets S3 — os 5 buckets do Data Lake Medalhão
# -----------------------------------------------------------------------------
echo ""
echo "=== Buckets S3 ==="
for BUCKET in ${PROJECT}-${ENV}-bronze ${PROJECT}-${ENV}-silver ${PROJECT}-${ENV}-gold ${PROJECT}-${ENV}-scripts ${PROJECT}-${ENV}-athena; do
  if aws s3api head-bucket --bucket $BUCKET --region $AWS_REGION 2>/dev/null; then
    echo "  ❌ Bucket $BUCKET: ainda existe"
    CLEAN=false
  else
    echo "  ✅ Bucket $BUCKET: deletado"
  fi
done

# -----------------------------------------------------------------------------
# 3. Lambda Producer
# -----------------------------------------------------------------------------
echo ""
echo "=== Lambda ==="
LAMBDA=$(aws lambda get-function --function-name ${PROJECT}-producer \
  --region $AWS_REGION --query 'Configuration.FunctionName' --output text 2>/dev/null)
if [ -z "$LAMBDA" ] || [ "$LAMBDA" == "None" ]; then
  echo "  ✅ Lambda producer: deletada"
else
  echo "  ❌ Lambda: $LAMBDA ainda existe"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# 4. Lambda Layer (confluent-kafka) — criado manualmente na seção 7 do setup
# -----------------------------------------------------------------------------
echo ""
echo "=== Lambda Layer ==="
LAYER=$(aws lambda list-layer-versions --layer-name kafka-layer \
  --region $AWS_REGION --query 'LayerVersions[0].Version' --output text 2>/dev/null)
if [ -z "$LAYER" ] || [ "$LAYER" == "None" ]; then
  echo "  ✅ Layer kafka-layer: deletado"
else
  echo "  ⚠️  Layer kafka-layer versão $LAYER ainda existe (sem custo, remoção opcional)"
fi

# -----------------------------------------------------------------------------
# 5. EventBridge
# -----------------------------------------------------------------------------
echo ""
echo "=== EventBridge ==="
for RULE in ${PROJECT}-producer-schedule ${PROJECT}-batch-trigger; do
  EB=$(aws events describe-rule --name $RULE --region $AWS_REGION \
    --query 'Name' --output text 2>/dev/null)
  if [ -z "$EB" ] || [ "$EB" == "None" ]; then
    echo "  ✅ Rule $RULE: deletada"
  else
    echo "  ❌ Rule $RULE: ainda existe"
    CLEAN=false
  fi
done

# -----------------------------------------------------------------------------
# 6. Glue Jobs — batch (3) + streaming (1)
# -----------------------------------------------------------------------------
echo ""
echo "=== Glue Jobs ==="
for JOB in ${PROJECT}-raw-to-bronze ${PROJECT}-bronze-to-silver ${PROJECT}-silver-to-gold ${PROJECT}-streaming; do
  J=$(aws glue get-job --job-name $JOB --region $AWS_REGION \
    --query 'Job.Name' --output text 2>/dev/null)
  if [ -z "$J" ] || [ "$J" == "None" ]; then
    echo "  ✅ Job $JOB: deletado"
  else
    echo "  ❌ Job $JOB: ainda existe"
    CLEAN=false
  fi
done

# -----------------------------------------------------------------------------
# 7. Glue Crawlers
# -----------------------------------------------------------------------------
echo ""
echo "=== Glue Crawlers ==="
for CRAWLER in ${PROJECT}-gold-crawler ${PROJECT}-streaming-crawler; do
  C=$(aws glue get-crawler --name $CRAWLER --region $AWS_REGION \
    --query 'Crawler.Name' --output text 2>/dev/null)
  if [ -z "$C" ] || [ "$C" == "None" ]; then
    echo "  ✅ Crawler $CRAWLER: deletado"
  else
    echo "  ❌ Crawler $CRAWLER: ainda existe"
    CLEAN=false
  fi
done

# -----------------------------------------------------------------------------
# 8. Glue Connection e Database
# -----------------------------------------------------------------------------
echo ""
echo "=== Glue Connection ==="
CONN=$(aws glue get-connection --name ${PROJECT}-msk-connection \
  --region $AWS_REGION --query 'Connection.Name' --output text 2>/dev/null)
if [ -z "$CONN" ] || [ "$CONN" == "None" ]; then
  echo "  ✅ Connection: deletada"
else
  echo "  ❌ Connection: $CONN ainda existe"
  CLEAN=false
fi

echo ""
echo "=== Glue Database ==="
DB=$(aws glue get-database --name ${PROJECT//-/_}_db \
  --region $AWS_REGION --query 'Database.Name' --output text 2>/dev/null)
if [ -z "$DB" ] || [ "$DB" == "None" ]; then
  echo "  ✅ Database: deletado"
else
  echo "  ❌ Database: $DB ainda existe"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# 9. Step Functions
# -----------------------------------------------------------------------------
echo ""
echo "=== Step Functions ==="
SFN=$(aws stepfunctions list-state-machines --region $AWS_REGION \
  --query "stateMachines[?name=='${PROJECT}-streaming-orchestrator'].name" \
  --output text 2>/dev/null)
if [ -z "$SFN" ] || [ "$SFN" == "None" ]; then
  echo "  ✅ State Machine: deletada"
else
  echo "  ❌ State Machine: $SFN ainda existe"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# 10. Athena Workgroup
# -----------------------------------------------------------------------------
echo ""
echo "=== Athena Workgroup ==="
WG=$(aws athena get-work-group --work-group ${PROJECT}-workgroup \
  --region $AWS_REGION --query 'WorkGroup.Name' --output text 2>/dev/null)
if [ -z "$WG" ] || [ "$WG" == "None" ]; then
  echo "  ✅ Workgroup: deletado"
else
  echo "  ❌ Workgroup: $WG ainda existe"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# 11. IAM Roles
# -----------------------------------------------------------------------------
echo ""
echo "=== IAM Roles ==="
for ROLE in GlueRole-${PROJECT} LambdaRole-${PROJECT} StepFunctionsRole-${PROJECT}; do
  EXISTS=$(aws iam get-role --role-name $ROLE \
    --query 'Role.RoleName' --output text 2>/dev/null)
  if [ -z "$EXISTS" ] || [ "$EXISTS" == "None" ]; then
    echo "  ✅ Role $ROLE: deletada"
  else
    echo "  ❌ Role $ROLE: ainda existe"
    CLEAN=false
  fi
done

# -----------------------------------------------------------------------------
# 12. VPC, Security Group e rede
# -----------------------------------------------------------------------------
echo ""
echo "=== VPC ==="
VPC=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=${PROJECT}-vpc" \
  --region $AWS_REGION --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
if [ -z "$VPC" ] || [ "$VPC" == "None" ]; then
  echo "  ✅ VPC: deletada"
else
  echo "  ❌ VPC: $VPC ainda existe"
  CLEAN=false
fi

echo ""
echo "=== Security Group ==="
SG=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=${PROJECT}-msk-sg" \
  --region $AWS_REGION --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
if [ -z "$SG" ] || [ "$SG" == "None" ]; then
  echo "  ✅ Security Group: deletado"
else
  echo "  ❌ Security Group: $SG ainda existe"
  CLEAN=false
fi

# -----------------------------------------------------------------------------
# Resultado final
# -----------------------------------------------------------------------------
echo ""
echo "============================================================"
if [ "$CLEAN" = true ]; then
  echo "  ✅ AMBIENTE LIMPO — nenhum recurso do projeto remanescente."
  echo "     Nenhum custo residual em andamento."
  echo "============================================================"
  exit 0
else
  echo "  ❌ RECURSOS PENDENTES — execute 'bash cleanup.sh' novamente"
  echo "     e rode esta verificação de novo."
  echo "============================================================"
  exit 1
fi
