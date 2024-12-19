from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_secretsmanager as secretsmanager,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3
)
from constructs import Construct


class QiitaEtlProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket.from_bucket_name(
            self,
            "qiita-etl-project-bucket",
            bucket_name="qiita-etl-project-bucket",
        )
        table = dynamodb.Table(
            self, "qiita-etl-project-articles",
            table_name="qiita-etl-project-articles",
            partition_key=dynamodb.Attribute(
                name="article_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Secret Manager作成
        secret = secretsmanager.Secret(self, "qiita-api-token",
                                       secret_name="qiita-api-token",
                                       generate_secret_string=secretsmanager.SecretStringGenerator(
                                           secret_string_template='{"api_token": ""}',
                                           generate_string_key="api_token"
                                       )
                                       )

        # Glueジョブ用のIAMロール
        glue_role = iam.Role(self, "glue-job-role",
                             role_name="glue-job-role",
                             assumed_by=iam.ServicePrincipal(
                                 "glue.amazonaws.com"),
                             managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                                 "service-role/AWSGlueServiceRole")]
                             )
        table.grant_read_write_data(glue_role)
        secret.grant_read(glue_role)

        # Glueジョブ
        glue_job = glue.CfnJob(self, "qiita-etl-job",
                               name="qiita-etl-job",
                               role=glue_role.role_arn,
                               command=glue.CfnJob.JobCommandProperty(
                                   name="pythonshell",
                                   python_version="3.9",
                                   script_location=f"s3://{bucket.bucket_name}/scripts/qiita_api.py",
                               ),
                               max_capacity=1,
                               default_arguments={
                                   "--secret_name": secret.secret_name,
                                   "--region_name": 'ap-northeast-1',
                                   "--s3_bucket_name": bucket.bucket_name,
                                   "--dynamodb_table_name": table.table_name,
                                   "--qiita_get_items_api_url": "https://qiita.com/api/v2/items",
                               },
                               glue_version='3.0',
                               timeout=100
                               )
        trigger = glue.CfnTrigger(self, "qiita-etl-trigger",
                                  name="qiita-etl-trigger",
                                  type="SCHEDULED",
                                  schedule='cron(0 8 ? * 2 *)',
                                  start_on_creation=True,
                                  actions=[glue.CfnTrigger.ActionProperty(
                                      job_name="qiita-etl-job",
                                      notification_property=glue.CfnTrigger.NotificationPropertyProperty(
                                          notify_delay_after=123
                                      ),
                                      security_configuration="securityConfiguration",
                                      timeout=123
                                  )],
                                  )
