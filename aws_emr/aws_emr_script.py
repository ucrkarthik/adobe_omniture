import boto3
import logging as log

session = boto3.session.Session(profile_name='kv_aws')
AWS_S3 = session.client('s3')
AWS_RES = session.resource('s3')
AWS_EMR = boto3.client('emr')


def create_cluster_helper(**kwargs):
    """
    Method to that creates a EMR cluster using boto3.
    :param kwargs: Key-value arguments: contains bootstrap object
    :return: Return cluster id
    """
    # bootstrap actions can be read from .json file or list like below or variable from cloudformation
    bootstrap_actions = [
        {
            'Name': 'pip36-install-dependencies',
            'ScriptBootstrapAction': {
                'Path': "s3://adobeomniture/bootstrap.sh",
                "Args": ["adobeomniture", "adobe-omniture-1.0.0"]
            }
        }
    ]

    response = AWS_EMR.run_job_flow(
        Name="Search Engine Revenu",
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Ganglia'}, {'Name': 'Zeppelin'}],
        LogUri="s3://adobeomniture/logs",
        ReleaseLabel="adobeomniture-1.0.0",
        VisibleToAllUsers=True,
        Configurations=[
            {
                'Classification': 'spark-defaults',
                'Configurations': [],
                'Properties': {
                    'spark.ssl.ui.enabled': 'false',
                }
            },
            {
                'Classification': 'yarn-site',
                'Configurations': [],
                'Properties': {
                    'yarn.resourcemanager.am.max-attempts': '1',
                }
            },
            {
                'Classification': 'core-site',
                'Configurations': [],
                'Properties': {
                    'fs.s3.canned.acl': 'BucketOwnerFullControl',
                }
            },
        ],
        Instances={
            'Ec2KeyName': "adobeomniture-test",
            'KeepJobFlowAliveWhenNoSteps': True,
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.2xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 64
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': True
                    }
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.2xlarge',
                    'InstanceCount': 5,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 64
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': True
                    }
                },
                {
                    'Name': 'Task',
                    'Market': 'SPOT',
                    'InstanceRole': 'TASK',
                    'BidPrice': 0.400,
                    'InstanceType': 'm5.2xlarge',
                    'InstanceCount': 5 // 2,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 64
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': True
                    }
                }
            ]
        },

        BootstrapActions=bootstrap_actions
    )

    cluster_id= response['JobFlowId']
    log.info("Cluster Id: " + cluster_id)
    return cluster_id


def get_step():
    return {
            "Name": "Adobe-Omniture",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--name",
                    "DataTransform",
                    "--executor-cores",
                    "5",
                    "--executor-memory",
                    "18g",
                    "--driver-memory",
                    "18g",
                    "--conf",
                    "spark.network.timeout=2400",
                    "--conf",
                    "spark.yarn.appMasterEnv.PYSPARK_PYTHON=python36",
                    "--conf",
                    "spark.executorEnv.PYSPARK_PYTHON=python36",
                    "--conf",
                    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
                    "--conf",
                    "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored=true",
                    "--conf",
                    "spark.hadoop.parquet.enable.summary-metadata=false",
                    "/usr/local/lib/python3.6/site-packages/adobe/omniture/driver.py",
                    "adobe/omniture/se_revenue/se_revenue_driver",
                    "--source=s3://adobeomniture/source/",
                    "--target=s3://adobeomniture/target/DATE_SearchKeywordPerformance.tab",
                    "--app_name=de-score-pipeline"
                ]
            }
        }


def main(main_args: list) -> None:

    response = AWS_EMR.add_job_flow_steps(
        JobFlowId=create_cluster_helper,
        Steps=[get_step]
    )

    log.info("Step Id: " + response['StepIds'][0])


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))