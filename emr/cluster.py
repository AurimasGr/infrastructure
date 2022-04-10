from clusterbase import ClusterBase


class SparkV1(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.17.0',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet_id',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://busket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                }
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkV2(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
                                    }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet_id',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class RandomExtraConfigs(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
                                    }
                            }
                            #                            {
                            #                                'Classification': 'spark-defaults',
                            #                                'Properties':
                            #                                    {
                            #                                        'spark.executor.memory': '10G'
                            #                                    }
                            #                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar',
                                 '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
                # {
                #     'Name': 'bash - printenv copy',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/printenv.sh', '/home/hadoop/']
                #     }
                # },
                # {
                #     'Name': 'bash - printenv',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['bash', '/home/hadoop/printenv.sh']
                #     }
                # }
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkVTroubleshoot(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.network.timeout': "800000",
                                        'spark.executor.heartbeatInterval': "800000"
                                    }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkV3(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": {
                                        "maximizeResourceAllocation": "false"
                                    }
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.executor.memory': '16g',
                                        'spark.executor.instances': '100',
                                        'spark.driver.memory': '16g',
                                        'spark.executor.cores': '4',
                                        'spark.network.timeout': "300",
                                        'spark.dynamicAllocation.enabled': 'false',
                                        'spark.sql.shuffle.partitions': '1000',
                                        'spark.default.parallelism': '1000',
                                        'spark.memory.fraction': '0.8',
                                        'spark.memory.storageFraction': '0.1',
                                        'spark.yarn.executor.memoryOverhead': "1600",
                                        'spark.yarn.driver.memoryOverhead': "1600"
                                    }
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": {
                                    "yarn.nodemanager.vmem-pmem-ratio": "4",
                                    "yarn.nodemanager.pmem-check-enabled": "false",
                                    "yarn.nodemanager.vmem-check-enabled": "false"
                                }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkV4(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1
                        # 'Configurations': [
                        #     {
                        #         'Classification': 'spark-env',
                        #         'Configurations': [
                        #             {
                        #                 'Classification': 'export',
                        #                 'Properties': {
                        #                     'PYSPARK_PYTHON': '/usr/bin/python3'
                        #                 }
                        #             }
                        #         ]
                        #     },
                        #     {
                        #         "Classification": "spark",
                        #             "Properties": self.spark or {}
                        #     },
                        #     {
                        #         'Classification': 'spark-defaults',
                        #         'Properties': self.spark_defaults or {}
                        #     },
                        #     {
                        #         "Classification": "yarn-site",
                        #         "Properties": self.yarn_site or {}
                        #     }
                        # ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                        # 'Configurations': [
                        #     {
                        #         'Classification': 'spark-env',
                        #         'Configurations': [
                        #             {
                        #                 'Classification': 'export',
                        #                 'Properties': {
                        #                     'PYSPARK_PYTHON': '/usr/bin/python3'
                        #                 }
                        #             }
                        #         ]
                        #     },
                        #     {
                        #         "Classification": "spark",
                        #             "Properties": self.spark or {}
                        #     },
                        #     {
                        #         'Classification': 'spark-defaults',
                        #         'Properties': self.spark_defaults or {}
                        #     },
                        #     {
                        #         "Classification": "yarn-site",
                        #         "Properties": self.yarn_site or {}
                        #     }
                        # ]
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'Configurations': [
                {
                    'Classification': 'spark-env',
                    'Configurations': [
                        {
                            'Classification': 'export',
                            'Properties': {
                                'PYSPARK_PYTHON': '/usr/bin/python3'
                            }
                        }
                    ]
                },
                {
                    "Classification": "spark",
                        "Properties": self.spark or {}
                },
                {
                    'Classification': 'spark-defaults',
                    'Properties': self.spark_defaults or {}
                },
                {
                    "Classification": "yarn-site",
                    "Properties": self.yarn_site or {}
                }
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkV5(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": {
                                        "maximizeResourceAllocation": "false"
                                    }
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.dynamicAllocation.enabled': 'false',
                                        'spark.executor.memory': '4g',
                                        'spark.yarn.executor.memoryOverhead': "800",
                                        'spark.executor.cores': '2',
                                        'spark.executor.instances': '534',
                                        'spark.driver.memory': '4g',
                                        'spark.yarn.driver.memoryOverhead': "800",
                                        # 'spark.driver.cores': '2',
                                        'spark.default.parallelism': '1334',
                                        'spark.sql.shuffle.partitions': '1334',
                                        'spark.network.timeout': "300",
                                        'spark.memory.fraction': '0.8',
                                        'spark.memory.storageFraction': '0.1'
                                    }
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": {
                                    "yarn.nodemanager.vmem-pmem-ratio": "4",
                                    "yarn.nodemanager.pmem-check-enabled": "false",
                                    "yarn.nodemanager.vmem-check-enabled": "false"
                                }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1,
                        # 'Configurations': [
                        #     {
                        #         'Classification': 'spark-env',
                        #         'Configurations': [
                        #             {
                        #                 'Classification': 'export',
                        #                 'Properties': {
                        #                     'PYSPARK_PYTHON': '/usr/bin/python3'
                        #                 }
                        #             }
                        #         ]
                        #     },
                        #     {
                        #         "Classification": "spark",
                        #             "Properties": {
                        #                 "maximizeResourceAllocation": "false"
                        #             }
                        #     },
                        #     {
                        #         'Classification': 'spark-defaults',
                        #         'Properties':
                        #             {
                        #                 'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                        #                 'spark.dynamicAllocation.enabled': 'false',
                        #                 'spark.executor.memory': '7g',
                        #                 'spark.yarn.executor.memoryOverhead': "800",
                        #                 'spark.executor.cores': '4',
                        #                 'spark.executor.instances': '191',
                        #                 'spark.driver.memory': '7g',
                        #                 'spark.yarn.driver.memoryOverhead': "800",
                        #                 'spark.driver.cores': '4',
                        #                 'spark.default.parallelism': '1696',
                        #                 'spark.sql.shuffle.partitions': '1696',
                        #                 'spark.network.timeout': "300",
                        #                 'spark.memory.fraction': '0.8',
                        #                 'spark.memory.storageFraction': '0.1'
                        #             }
                        #     },
                        #     {
                        #         "Classification": "yarn-site",
                        #         "Properties": {
                        #             "yarn.nodemanager.vmem-pmem-ratio": "4",
                        #             "yarn.nodemanager.pmem-check-enabled": "false",
                        #             "yarn.nodemanager.vmem-check-enabled": "false"
                        #         }
                        #     }
                        # ]
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkV6(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": {
                                        "maximizeResourceAllocation": "false"
                                    }
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.executor.memory': '4g',
                                        'spark.executor.instances': '400',
                                        'spark.driver.memory': '4g',
                                        'spark.executor.cores': '2',
                                        'spark.network.timeout': "300",
                                        'spark.dynamicAllocation.enabled': 'false',
                                        'spark.sql.shuffle.partitions': '1000',
                                        'spark.default.parallelism': '1000',
                                        'spark.memory.fraction': '0.8',
                                        'spark.memory.storageFraction': '0.1',
                                        'spark.yarn.executor.memoryOverhead': "1600",
                                        'spark.yarn.driver.memoryOverhead': "1600"
                                    }
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": {
                                    "yarn.nodemanager.vmem-pmem-ratio": "4",
                                    "yarn.nodemanager.pmem-check-enabled": "false",
                                    "yarn.nodemanager.vmem-check-enabled": "false"
                                }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkVBackup(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": {
                                        "maximizeResourceAllocation": "false"
                                    }
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.dynamicAllocation.enabled': 'false',
                                        'spark.executor.memory': '7g',
                                        'spark.yarn.executor.memoryOverhead': "800",
                                        'spark.executor.cores': '4',
                                        'spark.executor.instances': '191',
                                        'spark.driver.memory': '7g',
                                        'spark.yarn.driver.memoryOverhead': "800",
                                        'spark.driver.cores': '4',
                                        'spark.default.parallelism': '1696',
                                        'spark.sql.shuffle.partitions': '1696',
                                        'spark.network.timeout': "300",
                                        'spark.memory.fraction': '0.8',
                                        'spark.memory.storageFraction': '0.1'
                                    }
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": {
                                    "yarn.nodemanager.vmem-pmem-ratio": "4",
                                    "yarn.nodemanager.pmem-check-enabled": "false",
                                    "yarn.nodemanager.vmem-check-enabled": "false"
                                }
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": {
                                        "maximizeResourceAllocation": "false"
                                    }
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties':
                                    {
                                        'spark.delta.logStore.class': "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
                                        'spark.dynamicAllocation.enabled': 'false',
                                        'spark.executor.memory': '7g',
                                        'spark.yarn.executor.memoryOverhead': "800",
                                        'spark.executor.cores': '4',
                                        'spark.executor.instances': '191',
                                        'spark.driver.memory': '7g',
                                        'spark.yarn.driver.memoryOverhead': "800",
                                        'spark.driver.cores': '4',
                                        'spark.default.parallelism': '1696',
                                        'spark.sql.shuffle.partitions': '1696',
                                        'spark.network.timeout': "300",
                                        'spark.memory.fraction': '0.8',
                                        'spark.memory.storageFraction': '0.1'
                                    }
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": {
                                    "yarn.nodemanager.vmem-pmem-ratio": "4",
                                    "yarn.nodemanager.pmem-check-enabled": "false",
                                    "yarn.nodemanager.vmem-check-enabled": "false"
                                }
                            }
                        ]
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkVDynamic1(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            'Name': self.config['cluster_name'],
            'LogUri': 's3://bucket-name/emr/logs',
            'ReleaseLabel': 'emr-5.24.1',
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.instance_type_master or self.config['instance_type'],
                        'InstanceCount': 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": self.spark or {}
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties': self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.instance_type_worker or self.config['instance_type'],
                        'InstanceCount': self.config['instance_count'] - 1,
                        'Configurations': [
                            {
                                'Classification': 'spark-env',
                                'Configurations': [
                                    {
                                        'Classification': 'export',
                                        'Properties': {
                                            'PYSPARK_PYTHON': '/usr/bin/python3'
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                    "Properties": self.spark or {}
                            },
                            {
                                'Classification': 'spark-defaults',
                                'Properties': self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'submnet name',
                'Ec2KeyName': 'key.name'
            },
            'Applications': [
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ],
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': 'spark on-demand'
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Prepare Python environment',
                    'ScriptBootstrapAction': {
                        'Path': 's3://bucket-name/emr/bootstrap.sh'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Delta Lake - Copy Jar',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'aws', 's3', 'cp', 's3://bucket-name/emr/delta-core_2.11-0.2.0.jar', '/usr/lib/livy/repl_2.11-jars']
                    }
                },
                {
                    'Name': 'Apache Livy - Copy Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://bucket-name/emr/step-livy.sh', '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Apache Livy - Run Setup',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['bash', '/home/hadoop/step-livy.sh']
                    }
                },
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }


class SparkVDynamic2(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            "Name": self.config["cluster_name"],
            "LogUri": "s3://bucket-name/emr/logs",
            "ReleaseLabel": "emr-5.24.1",
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": self.instance_type_master or self.config["instance_type"],
                        "InstanceCount": 1,
                        "Configurations": [
                            {
                                "Classification": "spark-env",
                                "Configurations": [
                                    {
                                        "Classification": "export",
                                        "Properties": {
                                            "PYSPARK_PYTHON": "/usr/bin/python3"
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                "Properties": self.spark or {}
                            },
                            {
                                "Classification": "spark-defaults",
                                "Properties": self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    },
                    {
                        "Name": "Slave nodes",
                        "Market": self.core_instance_market or "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": self.instance_type_worker or self.config["instance_type"],
                        "InstanceCount": self.config["instance_count"] - 1,
                        "Configurations": [
                            {
                                "Classification": "spark-env",
                                "Configurations": [
                                    {
                                        "Classification": "export",
                                        "Properties": {
                                            "PYSPARK_PYTHON": "/usr/bin/python3"
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                "Properties": self.spark or {}
                            },
                            {
                                "Classification": "spark-defaults",
                                "Properties": self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    }
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2SubnetId": "submnet name",
                "Ec2KeyName": "key.name"
            },
            "Applications": [
                {"Name": "hadoop"},
                {'Name': "spark"},
                {"Name": "hive"},
                {"Name": "livy"},
                {"Name": "zeppelin"}
            ],
            "Tags": [
                {
                    "Key": "Name",
                    "Value": "spark on-demand"
                }
            ],
            "BootstrapActions": [
                {
                    "Name": "Prepare Python environment",
                    "ScriptBootstrapAction": {
                        "Path": "s3://bucket-name/emr/bootstrap.sh"
                    }
                }
            ],
            "Steps": [
                {
                    "Name": "Delta Lake - Copy Jar",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["sudo", "aws", "s3", "cp", "s3://bucket-name/emr/delta-core_2.11-0.2.0.jar", "/usr/lib/livy/repl_2.11-jars"]
                    }
                },
                {
                    "Name": "Apache Livy - Copy Setup",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["aws", "s3", "cp", "s3://bucket-name/emr/step-livy.sh", "/home/hadoop/"]
                    }
                },
                {
                    "Name": "Apache Livy - Run Setup",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["bash", "/home/hadoop/step-livy.sh"]
                    }
                },
            ],
            "VisibleToAllUsers": True,
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole"
        }


class SparkVDynamic3(ClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _set_cluster_config(self):

        self.cluster_config = {
            "Name": self.config["cluster_name"],
            "LogUri": "s3://bucket-name/emr/logs",
            "ReleaseLabel": "emr-5.24.1",
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": self.instance_type_master or self.config["instance_type"],
                        "InstanceCount": 1,
                        "Configurations": [
                            {
                                "Classification": "spark-env",
                                "Configurations": [
                                    {
                                        "Classification": "export",
                                        "Properties": {
                                            "PYSPARK_PYTHON": "/usr/bin/python3"
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                "Properties": self.spark or {}
                            },
                            {
                                "Classification": "spark-defaults",
                                "Properties": self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    },
                    {
                        "Name": "Slave nodes",
                        "Market": self.core_instance_market or "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": self.instance_type_worker or self.config["instance_type"],
                        "InstanceCount": self.config["instance_count"] - 1,
                        "Configurations": [
                            {
                                "Classification": "spark-env",
                                "Configurations": [
                                    {
                                        "Classification": "export",
                                        "Properties": {
                                            "PYSPARK_PYTHON": "/usr/bin/python3"
                                        }
                                    }
                                ]
                            },
                            {
                                "Classification": "spark",
                                "Properties": self.spark or {}
                            },
                            {
                                "Classification": "spark-defaults",
                                "Properties": self.spark_defaults or {}
                            },
                            {
                                "Classification": "yarn-site",
                                "Properties": self.yarn_site or {}
                            }
                        ]
                    }
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2SubnetId": "submnet-name",
                "Ec2KeyName": "key.name"
            },
            "Applications": [
                {"Name": "hadoop"},
                {'Name': "spark"},
                {"Name": "hive"},
                {"Name": "livy"},
                {"Name": "zeppelin"}
            ],
            "Tags": [
                {
                    "Key": "Name",
                    "Value": "spark on-demand"
                }
            ],
            "BootstrapActions": [
                {
                    "Name": "Prepare Python environment",
                    "ScriptBootstrapAction": {
                        "Path": "s3://bucket-name/emr/bootstrap.sh"
                    }
                }
            ],
            "Steps": [
                {
                    "Name": "Delta Lake - Copy Jar",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["sudo", "aws", "s3", "cp", "s3://bucket-name/emr/delta-core_2.11-0.6.1.jar", "/usr/lib/livy/repl_2.11-jars"]
                    }
                },
                {
                    "Name": "Apache Livy - Copy Setup",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["aws", "s3", "cp", "s3://bucket-name/emr/step-livy.sh", "/home/hadoop/"]
                    }
                },
                {
                    "Name": "Apache Livy - Run Setup",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["bash", "/home/hadoop/step-livy.sh"]
                    }
                },
            ],
            "VisibleToAllUsers": True,
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole"
        }