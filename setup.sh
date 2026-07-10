#!/usr/bin/env bash
# =============================================================================
# setup.sh — Pipeline Híbrido de Alfabetização no Brasil
# Tech Challenge Fase 2 — POSTECH AI Scientist
#
# Provisiona toda a infraestrutura via AWS CLI seguindo o padrão do lab FIAP
# (aulaKafka_provisioned.md). Execute seção por seção, aguardando cada etapa.
#
# Pré-requisitos:
#   aws configure  (credenciais com permissão de administrador)
#   python3, pip
#
# Tempo estimado: ~25 min (MSK leva ~15 min para ficar ACTIVE)
# Custo: ~US$0.14/hora enquanto o MSK estiver rodando — execute cleanup.sh
#         ao terminar para evitar cobranças desnecessárias.
# =============================================================================

set -euo pipefail

# =============================================================================
# VARIÁVEIS GLOBAIS
# =============================================================================

export AWS_REGION=us-east-1
export PROJECT=alfabetizacao-br
export ENV=dev

export ID_CONTA=$(aws sts get-caller-identity --query Account --output text)

export BUCKET_BRONZE=${PROJECT}-${ENV}-bronze
export BUCKET_SILVER=${PROJECT}-${ENV}-silver
export BUCKET_GOLD=${PROJECT}-${ENV}-gold
export BUCKET_SCRIPTS=${PROJECT}-${ENV}-scripts
export BUCKET_ATHENA=${PROJECT}-${ENV}-athena

export MSK_CLUSTER_NAME=${PROJECT}-msk
export MSK_TOPIC=alfabetizacao-br-streaming

export GLUE_DB=${PROJECT//-/_}_db          # alfabetizacao_br_db
export GLUE_ROLE=GlueRole-${PROJECT}
export LAMBDA_ROLE=LambdaRole-${PROJECT}
export SFN_ROLE=StepFunctionsRole-${PROJECT}

export VPC_NAME=${PROJECT}-vpc
export SG_NAME=${PROJECT}-msk-sg

echo "============================================================"
echo "  Projeto  : $PROJECT ($ENV)"
echo "  Conta    : $ID_CONTA"
echo "  Região   : $AWS_REGION"
echo "============================================================"

# =============================================================================
# SESSÃO PERDIDA? Recupere as variáveis com:
# =============================================================================
# export VPC_ID=$(aws ec2 describe-vpcs \
#   --filters "Name=tag:Name,Values=$VPC_NAME" \
#   --query 'Vpcs[0].VpcId' --output text --region $AWS_REGION)
# export SUBNET_1=$(aws ec2 describe-subnets \
#   --filters "Name=tag:Name,Values=${PROJECT}-subnet-1" \
#   --query 'Subnets[0].SubnetId' --output text --region $AWS_REGION)
# export SUBNET_2=$(aws ec2 describe-subnets \
#   --filters "Name=tag:Name,Values=${PROJECT}-subnet-2" \
#   --query 'Subnets[0].SubnetId' --output text --region $AWS_REGION)
# export SG_ID=$(aws ec2 describe-security-groups \
#   --filters "Name=group-name,Values=$SG_NAME" \
#   --query 'SecurityGroups[0].GroupId' --output text --region $AWS_REGION)
# export ROUTE_TABLE_ID=$(aws ec2 describe-route-tables \
#   --filters "Name=tag:Name,Values=${PROJECT}-rt" \
#   --query 'RouteTables[0].RouteTableId' --output text --region $AWS_REGION)
# export CLUSTER_ARN=$(aws kafka list-clusters --region $AWS_REGION \
#   --query "ClusterInfoList[?ClusterName=='$MSK_CLUSTER_NAME'].ClusterArn" --output text)
# export BOOTSTRAP=$(aws kafka get-bootstrap-brokers --cluster-arn $CLUSTER_ARN \
#   --region $AWS_REGION --query 'BootstrapBrokerString' --output text)

# =============================================================================
# 0. VPC, SUBNETS E REDE
# =============================================================================
# O MSK exige subnets privadas em pelo menos 2 AZs diferentes.
# O S3 VPC Endpoint permite que o Glue acesse o S3 sem sair da VPC.
echo ""
echo ">>> [0/10] Criando VPC e rede..."

export VPC_ID=$(aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${VPC_NAME}}]" \
  --region $AWS_REGION --query 'Vpc.VpcId' --output text)

aws ec2 modify-vpc-attribute --vpc-id $VPC_ID --enable-dns-hostnames --region $AWS_REGION
aws ec2 modify-vpc-attribute --vpc-id $VPC_ID --enable-dns-support   --region $AWS_REGION
echo "  VPC: $VPC_ID"

# 2 subnets em AZs diferentes (requisito do MSK)
AZ_ARRAY=($(aws ec2 describe-availability-zones \
  --filters "Name=state,Values=available" \
  --region $AWS_REGION --query 'AvailabilityZones[0:2].ZoneName' --output text))

export SUBNET_1=$(aws ec2 create-subnet --vpc-id $VPC_ID \
  --cidr-block 10.0.1.0/24 --availability-zone ${AZ_ARRAY[0]} \
  --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${PROJECT}-subnet-1}]" \
  --region $AWS_REGION --query 'Subnet.SubnetId' --output text)

export SUBNET_2=$(aws ec2 create-subnet --vpc-id $VPC_ID \
  --cidr-block 10.0.2.0/24 --availability-zone ${AZ_ARRAY[1]} \
  --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${PROJECT}-subnet-2}]" \
  --region $AWS_REGION --query 'Subnet.SubnetId' --output text)

echo "  Subnets: $SUBNET_1 (${AZ_ARRAY[0]})  $SUBNET_2 (${AZ_ARRAY[1]})"

# Internet Gateway + Route Table (Glue precisa baixar dependências)
export IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${PROJECT}-igw}]" \
  --region $AWS_REGION --query 'InternetGateway.InternetGatewayId' --output text)

aws ec2 attach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID --region $AWS_REGION

export ROUTE_TABLE_ID=$(aws ec2 create-route-table --vpc-id $VPC_ID \
  --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=${PROJECT}-rt}]" \
  --region $AWS_REGION --query 'RouteTable.RouteTableId' --output text)

aws ec2 associate-route-table --subnet-id $SUBNET_1 --route-table-id $ROUTE_TABLE_ID --region $AWS_REGION
aws ec2 associate-route-table --subnet-id $SUBNET_2 --route-table-id $ROUTE_TABLE_ID --region $AWS_REGION
aws ec2 create-route --route-table-id $ROUTE_TABLE_ID \
  --destination-cidr-block 0.0.0.0/0 --gateway-id $IGW_ID --region $AWS_REGION

echo "  IGW: $IGW_ID  |  Route Table: $ROUTE_TABLE_ID"

# S3 VPC Endpoint Gateway — tráfego Glue → S3 gratuito e sem sair da VPC
aws ec2 create-vpc-endpoint --vpc-id $VPC_ID \
  --vpc-endpoint-type Gateway \
  --service-name com.amazonaws.$AWS_REGION.s3 \
  --route-table-ids $ROUTE_TABLE_ID \
  --region $AWS_REGION \
  --tag-specifications "ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=${PROJECT}-s3-endpoint}]" \
  --query 'VpcEndpoint.VpcEndpointId' --output text

# =============================================================================
# 1. SECURITY GROUP
# =============================================================================
# Porta 9092 = Kafka PLAINTEXT | 2181 = ZooKeeper | self = workers do Glue
echo ""
echo ">>> [1/10] Criando Security Group..."

export SG_ID=$(aws ec2 create-security-group \
  --group-name $SG_NAME \
  --description "SG MSK + Glue workers — ${PROJECT}" \
  --vpc-id $VPC_ID \
  --region $AWS_REGION --query 'GroupId' --output text)

aws ec2 authorize-security-group-ingress --group-id $SG_ID \
  --protocol tcp --port 9092 --cidr 10.0.0.0/16 --region $AWS_REGION

aws ec2 authorize-security-group-ingress --group-id $SG_ID \
  --protocol tcp --port 2181 --cidr 10.0.0.0/16 --region $AWS_REGION

# Auto-referência: Glue workers no mesmo SG comunicam livremente com os brokers
aws ec2 authorize-security-group-ingress --group-id $SG_ID \
  --protocol tcp --port 0-65535 --source-group $SG_ID --region $AWS_REGION

echo "  Security Group: $SG_ID"

# =============================================================================
# 2. BUCKETS S3
# =============================================================================
echo ""
echo ">>> [2/10] Criando buckets S3..."

for BUCKET in $BUCKET_BRONZE $BUCKET_SILVER $BUCKET_GOLD $BUCKET_SCRIPTS $BUCKET_ATHENA; do
  aws s3api create-bucket --bucket $BUCKET --region $AWS_REGION 2>/dev/null || \
    echo "  (bucket $BUCKET já existe)"
  echo "  s3://$BUCKET"
done

# Lifecycle Bronze: dados brutos ficam 30 dias em Standard, depois IA
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET_BRONZE \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "bronze-lifecycle",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 90, "StorageClass": "GLACIER"}
      ]
    }]
  }'

echo "  Lifecycle Bronze configurado."

# =============================================================================
# 3. IAM ROLES
# =============================================================================
echo ""
echo ">>> [3/10] Criando IAM roles..."

# Role do Glue
cat > /tmp/glue-trust.json << 'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

aws iam create-role --role-name $GLUE_ROLE \
  --assume-role-policy-document file:///tmp/glue-trust.json 2>/dev/null || true
aws iam attach-role-policy --role-name $GLUE_ROLE \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam attach-role-policy --role-name $GLUE_ROLE \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name $GLUE_ROLE \
  --policy-arn arn:aws:iam::aws:policy/AmazonMSKFullAccess

export GLUE_ROLE_ARN=$(aws iam get-role --role-name $GLUE_ROLE --query 'Role.Arn' --output text)
echo "  Glue Role: $GLUE_ROLE_ARN"

# Role da Lambda (precisa de acesso à VPC para alcançar o MSK)
cat > /tmp/lambda-trust.json << 'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

aws iam create-role --role-name $LAMBDA_ROLE \
  --assume-role-policy-document file:///tmp/lambda-trust.json 2>/dev/null || true
aws iam attach-role-policy --role-name $LAMBDA_ROLE \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
aws iam attach-role-policy --role-name $LAMBDA_ROLE \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam attach-role-policy --role-name $LAMBDA_ROLE \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

export LAMBDA_ROLE_ARN=$(aws iam get-role --role-name $LAMBDA_ROLE --query 'Role.Arn' --output text)
echo "  Lambda Role: $LAMBDA_ROLE_ARN"

# Role do Step Functions
cat > /tmp/sfn-trust.json << 'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

aws iam create-role --role-name $SFN_ROLE \
  --assume-role-policy-document file:///tmp/sfn-trust.json 2>/dev/null || true
cat > /tmp/sfn-policy.json << 'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["glue:StartJobRun","glue:GetJobRun","glue:GetJobRuns","glue:BatchStopJobRun","glue:StartCrawler","glue:GetCrawler"],"Resource":"*"}]}
EOF
aws iam put-role-policy --role-name $SFN_ROLE \
  --policy-name GlueAccess --policy-document file:///tmp/sfn-policy.json

export SFN_ROLE_ARN=$(aws iam get-role --role-name $SFN_ROLE --query 'Role.Arn' --output text)
echo "  Step Functions Role: $SFN_ROLE_ARN"

# =============================================================================
# 4. CLUSTER MSK (PLAINTEXT, kafka.t3.small × 2)
# =============================================================================
# ⏱️ Esta etapa leva ~15 minutos. Passe para a seção 5 enquanto aguarda.
echo ""
echo ">>> [4/10] Criando cluster MSK... (aguarde ~15 min)"

aws kafka create-cluster \
  --cluster-name $MSK_CLUSTER_NAME \
  --kafka-version "3.7.x" \
  --number-of-broker-nodes 2 \
  --broker-node-group-info "{
    \"InstanceType\": \"kafka.t3.small\",
    \"ClientSubnets\": [\"$SUBNET_1\", \"$SUBNET_2\"],
    \"SecurityGroups\": [\"$SG_ID\"],
    \"StorageInfo\": {\"EbsStorageInfo\": {\"VolumeSize\": 10}}
  }" \
  --encryption-info "{\"EncryptionInTransit\":{\"ClientBroker\":\"PLAINTEXT\",\"InCluster\":false}}" \
  --region $AWS_REGION > /dev/null

echo "  MSK criando... verifique com:"
echo "  aws kafka list-clusters --region $AWS_REGION --query \"ClusterInfoList[?ClusterName=='$MSK_CLUSTER_NAME'].State\" --output text"

# =============================================================================
# 5. UPLOAD DOS SCRIPTS GLUE PARA S3
# =============================================================================
echo ""
echo ">>> [5/10] Fazendo upload dos scripts Glue para S3..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

aws s3 cp "$SCRIPT_DIR/glue_jobs/etl_bronze.py"    s3://$BUCKET_SCRIPTS/glue/etl_bronze.py    --region $AWS_REGION
aws s3 cp "$SCRIPT_DIR/glue_jobs/etl_silver.py"    s3://$BUCKET_SCRIPTS/glue/etl_silver.py    --region $AWS_REGION
aws s3 cp "$SCRIPT_DIR/glue_jobs/etl_gold.py"      s3://$BUCKET_SCRIPTS/glue/etl_gold.py      --region $AWS_REGION
aws s3 cp "$SCRIPT_DIR/glue_jobs/streaming_glue.py" s3://$BUCKET_SCRIPTS/glue/streaming_glue.py --region $AWS_REGION

echo "  Scripts enviados para s3://$BUCKET_SCRIPTS/glue/"

# =============================================================================
# 6. GLUE: DATABASE, JOBS, CRAWLERS E NETWORK CONNECTION
# =============================================================================
echo ""
echo ">>> [6/10] Configurando Glue (jobs, crawlers, connection)..."

# Data Catalog database
aws glue create-database \
  --database-input "{\"Name\":\"$GLUE_DB\"}" \
  --region $AWS_REGION 2>/dev/null || echo "  (database já existe)"

# Job RAW → Bronze
aws glue create-job \
  --name "${PROJECT}-raw-to-bronze" \
  --role $GLUE_ROLE_ARN \
  --glue-version "4.0" \
  --number-of-workers 2 --worker-type "G.1X" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET_SCRIPTS/glue/etl_bronze.py\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{
    \"--BUCKET_RAW\":\"s3://$BUCKET_BRONZE/raw\",
    \"--BUCKET_SOR\":\"$BUCKET_BRONZE\",
    \"--job-language\":\"python\"
  }" \
  --region $AWS_REGION > /dev/null
echo "  Job: ${PROJECT}-raw-to-bronze"

# Job Bronze → Silver
aws glue create-job \
  --name "${PROJECT}-bronze-to-silver" \
  --role $GLUE_ROLE_ARN \
  --glue-version "4.0" \
  --number-of-workers 2 --worker-type "G.1X" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET_SCRIPTS/glue/etl_silver.py\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{
    \"--BUCKET_SOR\":\"$BUCKET_BRONZE\",
    \"--BUCKET_SOT\":\"$BUCKET_SILVER\",
    \"--job-bookmark-option\":\"job-bookmark-enable\",
    \"--job-language\":\"python\"
  }" \
  --region $AWS_REGION > /dev/null
echo "  Job: ${PROJECT}-bronze-to-silver"

# Job Silver → Gold
aws glue create-job \
  --name "${PROJECT}-silver-to-gold" \
  --role $GLUE_ROLE_ARN \
  --glue-version "4.0" \
  --number-of-workers 2 --worker-type "G.1X" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET_SCRIPTS/glue/etl_gold.py\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{
    \"--BUCKET_SOT\":\"$BUCKET_SILVER\",
    \"--BUCKET_SPEC\":\"$BUCKET_GOLD\",
    \"--job-language\":\"python\"
  }" \
  --region $AWS_REGION > /dev/null
echo "  Job: ${PROJECT}-silver-to-gold"

# Glue Streaming Job (Spark Structured Streaming — aguarda MSK estar ACTIVE)
# O parâmetro --msk_bootstrap_servers é atualizado na seção 8, quando o MSK
# estiver ACTIVE e o bootstrap broker for conhecido.
aws glue create-job \
  --name "${PROJECT}-streaming" \
  --role $GLUE_ROLE_ARN \
  --glue-version "4.0" \
  --number-of-workers 2 --worker-type "G.1X" \
  --timeout 10 \
  --connections "{\"Connections\":[\"${PROJECT}-msk-connection\"]}" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET_SCRIPTS/glue/streaming_glue.py\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{
    \"--msk_bootstrap_servers\":\"PREENCHIDO_NA_SECAO_8\",
    \"--s3_output_path\":\"s3://$BUCKET_BRONZE/streaming/alfabetizacao/\",
    \"--checkpoint_path\":\"s3://$BUCKET_BRONZE/streaming/checkpoints/\",
    \"--job-language\":\"python\"
  }" \
  --region $AWS_REGION > /dev/null
echo "  Job: ${PROJECT}-streaming (msk_bootstrap_servers será atualizado na seção 8)"

# Crawler Gold
aws glue create-crawler \
  --name "${PROJECT}-gold-crawler" \
  --role $GLUE_ROLE_ARN \
  --database-name $GLUE_DB \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET_GOLD/gold/\"}]}" \
  --region $AWS_REGION 2>/dev/null || true
echo "  Crawler: ${PROJECT}-gold-crawler"

# Crawler Streaming (aponta para o path onde o streaming_glue.py grava)
aws glue create-crawler \
  --name "${PROJECT}-streaming-crawler" \
  --role $GLUE_ROLE_ARN \
  --database-name $GLUE_DB \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET_BRONZE/streaming/alfabetizacao/\"}]}" \
  --region $AWS_REGION 2>/dev/null || true
echo "  Crawler: ${PROJECT}-streaming-crawler"

# Glue Network Connection (coloca workers Spark dentro da VPC para acessar MSK)
SUBNET_AZ=$(aws ec2 describe-subnets --subnet-ids $SUBNET_1 \
  --region $AWS_REGION --query 'Subnets[0].AvailabilityZone' --output text)

aws glue create-connection --connection-input "{
  \"Name\": \"${PROJECT}-msk-connection\",
  \"ConnectionType\": \"NETWORK\",
  \"ConnectionProperties\": {},
  \"PhysicalConnectionRequirements\": {
    \"SubnetId\": \"$SUBNET_1\",
    \"SecurityGroupIdList\": [\"$SG_ID\"],
    \"AvailabilityZone\": \"$SUBNET_AZ\"
  }
}" --region $AWS_REGION 2>/dev/null || true
echo "  Glue Network Connection: ${PROJECT}-msk-connection"

# =============================================================================
# 7. LAMBDA — PRODUCER (batch ingestor é opcional, rode manualmente)
# =============================================================================
echo ""
echo ">>> [7/10] Criando Lambda Producer..."
echo "  ATENÇÃO: o Layer confluent-kafka precisa ser criado antes (veja README)."
echo "  Execute os passos abaixo separadamente:"
echo ""
echo "  # Baixar confluent-kafka compilado para Amazon Linux:"
echo "  mkdir -p /tmp/python"
echo "  pip install -t /tmp/python \\"
echo "    --platform manylinux2014_x86_64 --implementation cp \\"
echo "    --python-version 3.11 --only-binary=:all: \\"
echo "    'confluent-kafka==2.3.0' --quiet"
echo "  cd /tmp && zip -r kafka_layer.zip python"
echo "  aws s3 cp kafka_layer.zip s3://$BUCKET_SCRIPTS/layers/ --region $AWS_REGION"
echo ""
echo "  # Criar Layer no console Lambda:"
echo "  # Layers → Create layer → S3: s3://$BUCKET_SCRIPTS/layers/kafka_layer.zip"
echo "  # (anote o ARN do layer e coloque em LAYER_ARN abaixo)"
echo ""

# --- Após criar o Layer, descomente e execute: ---
# LAYER_ARN="arn:aws:lambda:$AWS_REGION:$ID_CONTA:layer:kafka-layer:1"

# zip -j /tmp/producer.zip lambda_functions/streaming_producer.py
# aws lambda create-function \
#   --function-name "${PROJECT}-producer" \
#   --runtime python3.11 \
#   --role $LAMBDA_ROLE_ARN \
#   --handler streaming_producer.lambda_handler \
#   --zip-file fileb:///tmp/producer.zip \
#   --timeout 120 --memory-size 256 \
#   --layers $LAYER_ARN \
#   --environment "Variables={MSK_BOOTSTRAP_SERVERS=PREENCHER_APOS_MSK_ATIVO,KAFKA_TOPIC=$MSK_TOPIC,MENSAGENS_POR_INVOCACAO=10}" \
#   --vpc-config "SubnetIds=$SUBNET_1,$SUBNET_2,SecurityGroupIds=$SG_ID" \
#   --region $AWS_REGION

echo "  (Lambda comentada — preencha LAYER_ARN e MSK_BOOTSTRAP_SERVERS após MSK ficar ACTIVE)"

# =============================================================================
# 8. AGUARDAR MSK E CAPTURAR BOOTSTRAP
# =============================================================================
echo ""
echo ">>> [8/10] Aguardando MSK ficar ACTIVE..."

while true; do
  STATE=$(aws kafka list-clusters --region $AWS_REGION \
    --query "ClusterInfoList[?ClusterName=='$MSK_CLUSTER_NAME'].State" --output text)
  echo "  Estado MSK: $STATE  $(date '+%H:%M:%S')"
  [ "$STATE" = "ACTIVE" ] && break
  sleep 60
done

export CLUSTER_ARN=$(aws kafka list-clusters --region $AWS_REGION \
  --query "ClusterInfoList[?ClusterName=='$MSK_CLUSTER_NAME'].ClusterArn" --output text)

export BOOTSTRAP=$(aws kafka get-bootstrap-brokers \
  --cluster-arn $CLUSTER_ARN --region $AWS_REGION \
  --query 'BootstrapBrokerString' --output text)

echo "  CLUSTER_ARN = $CLUSTER_ARN"
echo "  BOOTSTRAP   = $BOOTSTRAP"

# Atualiza o Glue Streaming Job com o bootstrap real
aws glue update-job \
  --job-name "${PROJECT}-streaming" \
  --job-update "{
    \"Role\": \"$GLUE_ROLE_ARN\",
    \"GlueVersion\": \"4.0\",
    \"NumberOfWorkers\": 2,
    \"WorkerType\": \"G.1X\",
    \"Timeout\": 10,
    \"Connections\": {\"Connections\":[\"${PROJECT}-msk-connection\"]},
    \"Command\": {\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET_SCRIPTS/glue/streaming_glue.py\",\"PythonVersion\":\"3\"},
    \"DefaultArguments\": {
      \"--msk_bootstrap_servers\": \"$BOOTSTRAP\",
      \"--s3_output_path\": \"s3://$BUCKET_BRONZE/streaming/alfabetizacao/\",
      \"--checkpoint_path\": \"s3://$BUCKET_BRONZE/streaming/checkpoints/\",
      \"--job-language\": \"python\"
    }
  }" --region $AWS_REGION > /dev/null
echo "  Glue Streaming Job atualizado com BOOTSTRAP=$BOOTSTRAP"

echo ""
echo "  ⚠️  Agora atualize a variável de ambiente da Lambda Producer:"
echo "  aws lambda update-function-configuration \\"
echo "    --function-name ${PROJECT}-producer \\"
echo "    --environment \"Variables={MSK_BOOTSTRAP_SERVERS=$BOOTSTRAP,KAFKA_TOPIC=$MSK_TOPIC,MENSAGENS_POR_INVOCACAO=10}\" \\"
echo "    --region $AWS_REGION"

# =============================================================================
# 9. EVENTBRIDGE + STEP FUNCTIONS + ATHENA
# =============================================================================
echo ""
echo ">>> [9/10] Criando EventBridge, Step Functions e Athena..."

# EventBridge — agenda a Lambda Producer a cada 5 minutos
aws events put-rule \
  --name "${PROJECT}-producer-schedule" \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED \
  --region $AWS_REGION > /dev/null
echo "  EventBridge rule: ${PROJECT}-producer-schedule (rate 5 min)"
echo "  Após criar a Lambda (seção 7), conecte-a à regra:"
echo "  aws lambda add-permission --function-name ${PROJECT}-producer \\"
echo "    --statement-id eventbridge-invoke --action lambda:InvokeFunction \\"
echo "    --principal events.amazonaws.com \\"
echo "    --source-arn arn:aws:events:$AWS_REGION:$ID_CONTA:rule/${PROJECT}-producer-schedule --region $AWS_REGION"
echo "  aws events put-targets --rule ${PROJECT}-producer-schedule \\"
echo "    --targets Id=1,Arn=arn:aws:lambda:$AWS_REGION:$ID_CONTA:function:${PROJECT}-producer --region $AWS_REGION"

# Step Functions — orquestra Glue Streaming + Crawler
SFN_DEF=$(cat << SFN
{
  "Comment": "Pipeline streaming: Glue Job → Aguarda → Para → Crawler",
  "StartAt": "IniciarGlueStreaming",
  "States": {
    "IniciarGlueStreaming": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun",
      "Parameters": {
        "JobName": "${PROJECT}-streaming",
        "Arguments": {
          "--msk_bootstrap_servers.$": "$.msk_bootstrap_servers",
          "--s3_output_path.$": "$.s3_output_path",
          "--checkpoint_path.$": "$.checkpoint_path"
        }
      },
      "ResultPath": "$.glue_run",
      "Next": "AguardarProcessamento"
    },
    "AguardarProcessamento": {
      "Type": "Wait",
      "Seconds": 300,
      "Next": "PararGlue"
    },
    "PararGlue": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:batchStopJobRun",
      "Parameters": {
        "JobName": "${PROJECT}-streaming",
        "JobRunIds.$": "States.Array($.glue_run.JobRunId)"
      },
      "Next": "IniciarCrawler",
      "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "IniciarCrawler"}]
    },
    "IniciarCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {"Name": "${PROJECT}-streaming-crawler"},
      "Next": "Sucesso"
    },
    "Sucesso": {
      "Type": "Succeed"
    }
  }
}
SFN
)

aws stepfunctions create-state-machine \
  --name "${PROJECT}-streaming-orchestrator" \
  --definition "$SFN_DEF" \
  --role-arn $SFN_ROLE_ARN \
  --region $AWS_REGION > /dev/null
echo "  Step Functions: ${PROJECT}-streaming-orchestrator"

# Athena Workgroup
aws athena create-work-group \
  --name "${PROJECT}-workgroup" \
  --configuration "{
    \"ResultConfiguration\":{\"OutputLocation\":\"s3://$BUCKET_ATHENA/results/\"},
    \"BytesScannedCutoffPerQuery\":1073741824
  }" \
  --region $AWS_REGION 2>/dev/null || true
echo "  Athena Workgroup: ${PROJECT}-workgroup (cutoff 1GB)"

# =============================================================================
# SEÇÃO 10 — UPLOAD DOS DADOS REAIS (INEP)
# =============================================================================
# Faz upload dos 5 arquivos CSV para o bucket Bronze (camada RAW).
# Por padrão usa a pasta ./dados do repositório; sobrescreva com DATA_DIR.
# =============================================================================

echo ""
echo ">>> [10/10] Upload dos dados INEP para o bucket Bronze..."

DATA_DIR="${DATA_DIR:-$SCRIPT_DIR/dados}"

ARQUIVOS_INEP=(
  "br_inep_avaliacao_alfabetizacao_municipio.csv"
  "br_inep_avaliacao_alfabetizacao_uf.csv"
  "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv"
  "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv"
  "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv"
)

for ARQUIVO in "${ARQUIVOS_INEP[@]}"; do
  LOCAL="${DATA_DIR}/${ARQUIVO}"
  if [ -f "$LOCAL" ]; then
    aws s3 cp "$LOCAL" "s3://${BUCKET_BRONZE}/raw/${ARQUIVO}" --region $AWS_REGION
    echo "  Upload OK: ${ARQUIVO}"
  else
    echo "  ⚠️  Arquivo não encontrado: $LOCAL (DATA_DIR=$DATA_DIR)"
  fi
done

echo "  Arquivos disponíveis em s3://${BUCKET_BRONZE}/raw/"

# =============================================================================
# FIM
# =============================================================================
echo ""
echo "============================================================"
echo "  ✅ Infraestrutura provisionada com sucesso!"
echo ""
echo "  BOOTSTRAP MSK : $BOOTSTRAP"
echo "  Bronze bucket : s3://$BUCKET_BRONZE"
echo "  Silver bucket : s3://$BUCKET_SILVER"
echo "  Gold bucket   : s3://$BUCKET_GOLD"
echo ""
echo "  PRÓXIMOS PASSOS:"
echo "  1. Crie o Lambda Layer e descomente a seção 7 deste script"
echo "  2. Atualize MSK_BOOTSTRAP_SERVERS na Lambda Producer"
echo "  3. Execute o pipeline batch (aguarde cada job concluir):"
echo "     aws glue start-job-run --job-name ${PROJECT}-raw-to-bronze"
echo "     aws glue start-job-run --job-name ${PROJECT}-bronze-to-silver"
echo "     aws glue start-job-run --job-name ${PROJECT}-silver-to-gold"
echo "     aws glue start-crawler --name ${PROJECT}-gold-crawler"
echo "  4. Execute o pipeline streaming (Step Functions):"
SFN_ARN=$(aws stepfunctions list-state-machines --region $AWS_REGION \
  --query "stateMachines[?name=='${PROJECT}-streaming-orchestrator'].stateMachineArn" --output text)
echo "     aws stepfunctions start-execution \\"
echo "       --state-machine-arn $SFN_ARN \\"
echo "       --input '{\"msk_bootstrap_servers\":\"$BOOTSTRAP\",\"s3_output_path\":\"s3://$BUCKET_BRONZE/streaming/alfabetizacao/\",\"checkpoint_path\":\"s3://$BUCKET_BRONZE/streaming/checkpoints/\"}'"
echo ""
echo "  ⚠️  Execute cleanup.sh ao terminar para evitar cobranças!"
echo "============================================================"
