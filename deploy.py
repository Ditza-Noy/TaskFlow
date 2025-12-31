# deploy.py
import boto3
from dotenv import load_dotenv

class CloudFormationDeployer:
    """Deploy TaskFlow infrastructure using CloudFormation."""

    def __init__(self, environment: str = "dev"):
        self.environment = environment
        load_dotenv()
        self.cloudformation = boto3.client('cloudformation', region_name='eu-west-1')
        self.stack_name = f"taskflow-{environment}"

    def deploy_infrastructure(self, template_path: str, parameters: dict[str, str] | None = None):
        """Deploy CloudFormation stack."""
        with open(template_path, 'r') as template_file:
            template_body = template_file.read()

        # Prepare parameters
        cf_parameters: list[dict[str, str]] = []
        if parameters:
            for key, value in parameters.items():
                cf_parameters.append({
                    'ParameterKey': key,
                    'ParameterValue': value
                })

        try:
            # Check if stack exists
            try:
                self.cloudformation.describe_stacks(StackName=self.stack_name)
                stack_exists = True
            except self.cloudformation.exceptions.ClientError:
                stack_exists = False

            if stack_exists:
                print(f"Updating existing stack: {self.stack_name}")
                response = self.cloudformation.update_stack(
                    StackName=self.stack_name,
                    TemplateBody=template_body,
                    Parameters=cf_parameters,
                    Capabilities=['CAPABILITY_IAM']
                )
                waiter = self.cloudformation.get_waiter('stack_update_complete')
            else:
                print(f"Creating new stack: {self.stack_name}")
                response = self.cloudformation.create_stack(
                    StackName=self.stack_name,
                    TemplateBody=template_body,
                    Parameters=cf_parameters,
                    Capabilities=['CAPABILITY_IAM'],
                    Tags=[
                        {'Key': 'Environment', 'Value': self.environment},
                        {'Key': 'Project', 'Value': 'TaskFlow'},
                        {'Key': 'ManagedBy', 'Value': 'CloudFormation'}
                    ]
                )
                waiter = self.cloudformation.get_waiter('stack_create_complete')

            print("Waiting for stack operation to complete...")
            waiter.wait(
                StackName=self.stack_name,
                WaiterConfig={'Delay': 30, 'MaxAttempts': 40}
            )
            print("Stack operation completed successfully!")
            return self.get_stack_outputs()
        except Exception as e:
            print(f"\n❌ Deployment failed: {e}")
            try:
                print("\n🔍 Fetching failure details from AWS...")
                events = self.cloudformation.describe_stack_events(StackName=self.stack_name)
                # print the most recent failed events first
                for event in events['StackEvents']:
                    if 'ResourceStatus' in event and 'FAILED' in event['ResourceStatus']:
                        print(f"   • Resource: {event.get('LogicalResourceId')}")
                        print(f"   • Reason:   {event.get('ResourceStatusReason')}")
                        # usually the first failure is the root cause, so we can break or keep listing
            except Exception as inner_e:
                print(f"   Could not fetch stack events: {inner_e}")
            # -----------------------------------------------
            
            raise

    def get_stack_outputs(self) -> dict[str, str]:
        """Get CloudFormation stack outputs."""
        try:
            response = self.cloudformation.describe_stacks(StackName=self.stack_name)
            outputs = {}
            for output in response['Stacks'][0].get('Outputs', []):
                outputs[output['OutputKey']] = output['OutputValue']
            return outputs
        except Exception as e:
            print(f"Error getting stack outputs: {e}")
            return {}


def main():
    """Main deployment script."""
    import argparse

    parser = argparse.ArgumentParser(description='Deploy TaskFlow infrastructure')
    parser.add_argument('--environment', default='dev', choices=['dev', 'staging', 'prod'])
    parser.add_argument('--db-password', required=True, help='Database password')
    args = parser.parse_args()

    deployer = CloudFormationDeployer(args.environment)
    parameters = {
        'Environment': args.environment,
        'ProjectName': 'taskflow',
        'DatabasePassword': args.db_password
    }

    outputs = deployer.deploy_infrastructure(
        'cloudformation/taskflow-infrastructure.yaml',
        parameters
    )

    print("\nStack outputs:")
    for key, value in outputs.items():
        print(f" {key}: {value}")

    # Update environment file
    with open('.env', 'a') as env_file:
        env_file.write(f"\n# CloudFormation outputs for {args.environment}\n")
        env_file.write(f"S3_BUCKET_NAME={outputs.get('BucketName', '')}\n")
        env_file.write(f"SQS_QUEUE_NAME={(outputs.get('QueueUrl', '')).split('/')[-1]}\n")
        env_file.write(f"RDS_HOST={outputs.get('DatabaseEndpoint', '')}\n")
        env_file.write(f"RDS_PORT={outputs.get('DatabasePort', '5432')}\n")


if __name__ == "__main__":
    main()
