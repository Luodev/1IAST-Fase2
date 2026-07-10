#!/usr/bin/env bash
# =============================================================================
# cleanup.sh — Remove todos os recursos AWS do projeto
# Tech Challenge Fase 2 — POSTECH AI Scientist
#
# ⚠️  Execute ao terminar cada sessão de testes.
#     O MSK cobra ~US$0.14/hora por cluster mesmo sem uso.
# =============================================================================

set -euo pipefail

export AWS_REGION=us-east-1
export PROJECT=alfabetizacao-br

echo "============================================================"
echo "  LIMPEZA — $PROJECT"
echo "  Região: $AWS_REGION"
echo "============================================================"
echo ""
read -p "Confirma? Isso remove TUDO (MSK, Glue, Lambda, S3, VPC). [s/N] " CONF
[ "${CONF,,}" != "s" ] && echo "Cancelado." && exit 0

# EventBridge
echo ">>> Removendo EventBridge..."
for RULE in "${PROJECT}-producer-schedule" "${PROJECT}-batch-trigger"; do
  aws events remove-targets --rule "$RULE" --ids 1 --region $AWS_REGION 2>/dev/null || true
  aws events delete-rule --name "$RULE" --region $AWS_REGION 2>/dev/null || true
done

# Step Functions
echo ">>> Removendo Step Functions..."
SFN_ARN=$(aws stepfunctions list-state-machines --region $AWS_REGION \
  --query "stateMachines[?name=='${PROJECT}-streaming-orchestrator'].stateMachineArn" --output text 2>/dev/null)
[ -n "$SFN_ARN" ] && aws stepfunctions delete-state-machine --state-machine-arn $SFN_ARN --region $AWS_REGION 2>/dev/null || true

# Lambda
echo ">>> Removendo Lambda..."
aws lambda delete-function --function-name "${PROJECT}-producer"       --region $AWS_REGION 2>/dev/null || true
aws lambda delete-function --function-name "${PROJECT}-batch-ingestor" --region $AWS_REGION 2>/dev/null || true

# Glue
echo ">>> Removendo Glue jobs, crawlers e connection..."
for JOB in "${PROJECT}-raw-to-bronze" "${PROJECT}-bronze-to-silver" "${PROJECT}-silver-to-gold" "${PROJECT}-streaming"; do
  aws glue delete-job --job-name $JOB --region $AWS_REGION 2>/dev/null || true
done
for CRAWLER in "${PROJECT}-gold-crawler" "${PROJECT}-streaming-crawler"; do
  aws glue delete-crawler --name $CRAWLER --region $AWS_REGION 2>/dev/null || true
done
aws glue delete-connection --connection-name "${PROJECT}-msk-connection" --region $AWS_REGION 2>/dev/null || true
aws glue delete-database --name "${PROJECT//-/_}_db" --region $AWS_REGION 2>/dev/null || true

# Athena
echo ">>> Removendo Athena workgroup..."
aws athena delete-work-group --work-group "${PROJECT}-workgroup" --recursive-delete-option \
  --region $AWS_REGION 2>/dev/null || true

# MSK
echo ">>> Removendo MSK (aguarde ~10 min para deleção completar)..."
CLUSTER_ARN=$(aws kafka list-clusters --region $AWS_REGION \
  --query "ClusterInfoList[?ClusterName=='${PROJECT}-msk'].ClusterArn" --output text 2>/dev/null)
if [ -n "$CLUSTER_ARN" ]; then
  aws kafka delete-cluster --cluster-arn $CLUSTER_ARN --region $AWS_REGION > /dev/null
  echo "  MSK deletando... aguardando..."
  while true; do
    STATE=$(aws kafka list-clusters --region $AWS_REGION \
      --query "ClusterInfoList[?ClusterName=='${PROJECT}-msk'].State" --output text 2>/dev/null)
    [ -z "$STATE" ] && echo "  MSK removido." && break
    echo "  Estado: $STATE  $(date '+%H:%M:%S')"
    sleep 30
  done
fi

# S3
echo ">>> Esvaziando e removendo buckets S3..."
for BUCKET in ${PROJECT}-dev-bronze ${PROJECT}-dev-silver ${PROJECT}-dev-gold ${PROJECT}-dev-scripts ${PROJECT}-dev-athena; do
  aws s3 rm s3://$BUCKET --recursive --region $AWS_REGION 2>/dev/null || true
  aws s3api delete-bucket --bucket $BUCKET --region $AWS_REGION 2>/dev/null || true
  echo "  Removido: s3://$BUCKET"
done

# IAM
echo ">>> Removendo IAM roles..."
for ROLE in GlueRole-${PROJECT} LambdaRole-${PROJECT} StepFunctionsRole-${PROJECT}; do
  # Detach all policies
  POLICIES=$(aws iam list-attached-role-policies --role-name $ROLE \
    --query 'AttachedPolicies[*].PolicyArn' --output text 2>/dev/null || true)
  for P in $POLICIES; do
    aws iam detach-role-policy --role-name $ROLE --policy-arn $P 2>/dev/null || true
  done
  # Delete inline policies
  INLINE=$(aws iam list-role-policies --role-name $ROLE \
    --query 'PolicyNames' --output text 2>/dev/null || true)
  for P in $INLINE; do
    aws iam delete-role-policy --role-name $ROLE --policy-name $P 2>/dev/null || true
  done
  aws iam delete-role --role-name $ROLE 2>/dev/null || true
  echo "  Removida role: $ROLE"
done

# VPC e rede
echo ">>> Removendo VPC e rede..."
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=tag:Name,Values=${PROJECT}-vpc" \
  --query 'Vpcs[0].VpcId' --output text --region $AWS_REGION 2>/dev/null || true)

if [ -n "$VPC_ID" ] && [ "$VPC_ID" != "None" ]; then
  SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${PROJECT}-msk-sg" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text --region $AWS_REGION 2>/dev/null || true)
  [ -n "$SG_ID" ] && [ "$SG_ID" != "None" ] && \
    aws ec2 delete-security-group --group-id $SG_ID --region $AWS_REGION 2>/dev/null || true

  IGW_ID=$(aws ec2 describe-internet-gateways \
    --filters "Name=attachment.vpc-id,Values=$VPC_ID" \
    --query 'InternetGateways[0].InternetGatewayId' --output text --region $AWS_REGION 2>/dev/null || true)
  if [ -n "$IGW_ID" ] && [ "$IGW_ID" != "None" ]; then
    aws ec2 detach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID --region $AWS_REGION 2>/dev/null || true
    aws ec2 delete-internet-gateway --internet-gateway-id $IGW_ID --region $AWS_REGION 2>/dev/null || true
  fi

  SUBNETS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'Subnets[*].SubnetId' --output text --region $AWS_REGION 2>/dev/null || true)
  for S in $SUBNETS; do
    aws ec2 delete-subnet --subnet-id $S --region $AWS_REGION 2>/dev/null || true
  done

  ENDPOINTS=$(aws ec2 describe-vpc-endpoints \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=vpc-endpoint-state,Values=available" \
    --query 'VpcEndpoints[*].VpcEndpointId' --output text --region $AWS_REGION 2>/dev/null || true)
  for E in $ENDPOINTS; do
    aws ec2 delete-vpc-endpoints --vpc-endpoint-ids $E --region $AWS_REGION 2>/dev/null || true
  done

  aws ec2 delete-vpc --vpc-id $VPC_ID --region $AWS_REGION 2>/dev/null || true
  echo "  VPC $VPC_ID removida."
fi

echo ""
echo "============================================================"
echo "  ✅ Limpeza concluída! Todos os recursos foram removidos."
echo "============================================================"
