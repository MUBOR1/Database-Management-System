#!/bin/bash

# ============================================
# AWS EMR Commands for HBase Lab
# ============================================

# 1. Настройка AWS CLI
aws configure

# 2. Создание ролей по умолчанию
aws emr create-default-roles

# 3. Создание ключа SSH
aws ec2 create-key-pair \
  --key-name HBaseShell \
  --query 'KeyMaterial' \
  --output text > ~/.ssh/hbase-shell-key.pem
chmod 400 ~/.ssh/hbase-shell-key.pem

# 4. Создание кластера HBase
CLUSTER_ID=$(aws emr create-cluster \
  --name "HBase Lab Cluster" \
  --release-label emr-5.3.1 \
  --ec2-attributes KeyName=HBaseShell \
  --use-default-roles \
  --instance-type m1.large \
  --instance-count 3 \
  --applications Name=HBase \
  --query 'ClusterId' \
  --output text)

echo "Cluster ID: $CLUSTER_ID"
export CLUSTER_ID=$CLUSTER_ID

# 5. Проверка статуса кластера
while true; do
  STATE=$(aws emr describe-cluster \
    --cluster-id $CLUSTER_ID \
    --query 'Cluster.Status.State' \
    --output text)
  echo "Cluster state: $STATE"
  if [ "$STATE" = "WAITING" ] || [ "$STATE" = "RUNNING" ]; then
    break
  fi
  sleep 30
done

# 6. Получение ID группы безопасности
SECURITY_GROUP_ID=$(aws emr describe-cluster \
  --cluster-id $CLUSTER_ID \
  --query 'Cluster.Ec2InstanceAttributes.EmrManagedMasterSecurityGroup' \
  --output text)

echo "Security Group ID: $SECURITY_GROUP_ID"
export SECURITY_GROUP_ID=$SECURITY_GROUP_ID

# 7. Получение текущего IP
MY_CIDR=$(dig +short myip.opendns.com @resolver1.opendns.com.)/32
echo "My IP: $MY_CIDR"

# 8. Разрешение SSH доступа
aws ec2 authorize-security-group-ingress \
  --group-id $SECURITY_GROUP_ID \
  --protocol tcp \
  --port 22 \
  --cidr $MY_CIDR

# 9. Подключение к кластеру
aws emr ssh \
  --cluster-id $CLUSTER_ID \
  --key-pair-file ~/.ssh/hbase-shell-key.pem

# 10. После подключения через SSH:
# $ hbase shell

# 11. Масштабирование кластера
# Получить ID группы инстансов Core
CORE_GROUP_ID=$(aws emr list-instance-groups \
  --cluster-id $CLUSTER_ID \
  --query 'InstanceGroups[?InstanceGroupType==`CORE`].Id' \
  --output text)

# Уменьшить до 1 Core node
aws emr modify-instance-groups \
  --instance-groups InstanceGroupId=$CORE_GROUP_ID,InstanceCount=1

# Увеличить до 2 Core nodes
aws emr modify-instance-groups \
  --instance-groups InstanceGroupId=$CORE_GROUP_ID,InstanceCount=2

# 12. Завершение работы кластера
aws emr terminate-clusters --cluster-ids $CLUSTER_ID