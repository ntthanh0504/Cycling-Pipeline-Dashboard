#!/bin/sh

# Check if the TERRAFORM_ACTION environment variable is set to 'destroy'
if [ "$TERRAFORM_ACTION" = "destroy" ]; then
  echo "Running Terraform Destroy..."
  terraform destroy -auto-approve
else
  echo "Running Terraform Apply..."
  terraform init && terraform apply -auto-approve
fi