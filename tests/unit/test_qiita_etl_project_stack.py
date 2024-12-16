import aws_cdk as core
import aws_cdk.assertions as assertions

from qiita_etl_project.qiita_etl_project_stack import QiitaEtlProjectStack

# example tests. To run these tests, uncomment this file along with the example
# resource in qiita_etl_project/qiita_etl_project_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = QiitaEtlProjectStack(app, "qiita-etl-project")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
