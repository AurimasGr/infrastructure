import boto3
import os


class ConfigBase:

    def __init__(self, **kwargs):

        self.optional_params = ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 'es_timeout']

        self.config = {}
        self.set_config(**kwargs)
        self.assert_config(self.config)

    @property
    def optional_parameters(self):
        return self.optional_params

    def assert_config(self, config):

        assert (all(config[x] is not None for x in config if x not in self.optional_parameters)), "Missing env variables."

    def set_config(self, **kwargs):

        self.config = self.config


class EMRCluster(ConfigBase):

    def __init__(self, **kwargs):

        self.emr_instance_count = None
        self.emr_instance_type = None
        self.emr_cluster_name = None
        self.emr_config_version = None
        self.emr_aws_access_key_id = None
        self.emr_aws_secret_access_key = None
        self.emr_aws_region = None

        super().__init__(**kwargs)

    def set_config(self, emr_instance_count_ev="EMR_INSTANCE_COUNT",
                   emr_instance_type_ev="EMR_INSTANCE_TYPE",
                   emr_cluster_name_ev="EMR_CLUSTER_NAME",
                   emr_config_version_ev="EMR_CONFIG_VERSION",
                   emr_aws_access_key_id_ev="AWS_ACCESS_KEY_ID",
                   emr_aws_secret_access_key_ev="AWS_SECRET_ACCESS_KEY",
                   emr_aws_region_ev="AWS_REGION"):

        self.emr_instance_count = int(os.getenv(emr_instance_count_ev, 3))
        self.emr_instance_type = os.getenv(emr_instance_type_ev, 'm4.large')
        self.emr_cluster_name = os.getenv(emr_cluster_name_ev, 'Spark On-Demand')
        self.emr_config_version = os.getenv(emr_config_version_ev)
        self.emr_aws_access_key_id = os.getenv(emr_aws_access_key_id_ev)
        self.emr_aws_secret_access_key = os.getenv(emr_aws_secret_access_key_ev)
        self.emr_aws_region = os.getenv(emr_aws_region_ev)

        self.config = {
            'instance_count': self.emr_instance_count,
            'instance_type': self.emr_instance_type,
            'cluster_name': self.emr_cluster_name,
            'config_version': self.emr_config_version,
            'aws_access_key_id': self.emr_aws_access_key_id,
            'aws_secret_access_key': self.emr_aws_secret_access_key,
            'aws_region': self.emr_aws_region
        }


class ClusterBase:

    def __init__(self, instance_type_master=None, instance_type_worker=None, core_instance_market=None, spark_defaults={}, yarn_site={}, spark={}, **kwargs):

        self.instance_type_master = instance_type_master
        self.instance_type_worker = instance_type_worker

        self.core_instance_market = core_instance_market

        self.spark_defaults = spark_defaults
        self.yarn_site = yarn_site
        self.spark = spark

        self.config_object = EMRCluster(**kwargs)
        self.config = self.config_object.config
        self.cluster_config = None
        self._set_client()
        self._set_cluster_config()

        print('Waiting for the EMR cluster to boot...', flush=True)

        spin_up_response = self.client.run_job_flow(Name=self.cluster_config["Name"],
                                                    LogUri=self.cluster_config["LogUri"],
                                                    ReleaseLabel=self.cluster_config["ReleaseLabel"],
                                                    Instances=self.cluster_config["Instances"],
                                                    Applications=self.cluster_config["Applications"],
                                                    Tags=self.cluster_config["Tags"],
                                                    BootstrapActions=self.cluster_config["BootstrapActions"],
                                                    Steps=self.cluster_config["Steps"],
                                                    VisibleToAllUsers=self.cluster_config["VisibleToAllUsers"],
                                                    JobFlowRole=self.cluster_config["JobFlowRole"],
                                                    ServiceRole=self.cluster_config["ServiceRole"])

        self.id = spin_up_response['JobFlowId']
        self.client.get_waiter('cluster_running').wait(ClusterId=self.id)
        final_step_id = self.client.list_steps(ClusterId=self.id)['Steps'][0]['Id']
        self.client.get_waiter('step_complete').wait(ClusterId=self.id, StepId=final_step_id)

        print(f'...cluster ({self.id}) is now running.', flush=True)

    def _set_client(self):
        self.client = boto3.client('emr', region_name=self.config['aws_region'])

    def _set_cluster_config(self):
        self.cluster_config = {"Name": "",
                               "LogUri": "",
                               "ReleaseLabel": "",
                               "Instances": {},
                               "Applications": [],
                               "Tags": [],
                               "BootstrapActions": [],
                               "Steps": [],
                               "VisibleToAllUsers": True,
                               "JobFlowRole": "",
                               "ServiceRole": ""}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f'Terminating the cluster ({self.id}).', flush=True)
        self.terminate()

    @property
    def dns(self):
        description_response = self.client.describe_cluster(ClusterId=self.id)
        return description_response['Cluster']['MasterPublicDnsName']

    def terminate(self):
        self.client.terminate_job_flows(JobFlowIds=[self.id])
