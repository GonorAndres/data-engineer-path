# =============================================================================
# Terraform Convenience Targets
# =============================================================================
# Usage:
#   make init          Initialize Terraform (download providers, set up backend)
#   make plan          Preview infrastructure changes
#   make apply         Apply changes (with confirmation prompt)
#   make destroy       Destroy all resources (with confirmation prompt)
#   make fmt           Format all .tf files
#   make validate      Validate configuration syntax
#   make plan-destroy  Preview what would be destroyed
#   make clean         Remove local Terraform cache
# =============================================================================

.PHONY: init plan apply destroy fmt validate plan-destroy clean output

# Default tfvars file
TFVARS ?= terraform.tfvars

init:
	terraform init

plan:
	terraform plan -var-file="$(TFVARS)"

apply:
	terraform apply -var-file="$(TFVARS)"

destroy:
	terraform destroy -var-file="$(TFVARS)"

plan-destroy:
	terraform plan -destroy -var-file="$(TFVARS)"

fmt:
	terraform fmt -recursive

validate:
	terraform validate

clean:
	rm -rf .terraform
	rm -f .terraform.lock.hcl

output:
	terraform output
