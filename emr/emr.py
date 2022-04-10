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


class EMRUtils(ConfigBase):

    def __init__(self, **kwargs):

        self.emr_aws_access_key_id = None
        self.emr_aws_secret_access_key = None
        self.emr_aws_region = None

        super().__init__(**kwargs)

    def set_config(self, emr_aws_access_key_id_ev="AWS_ACCESS_KEY_ID",
                   emr_aws_secret_access_key_ev="AWS_SECRET_ACCESS_KEY",
                   emr_aws_region_ev="AWS_REGION"):

        self.emr_aws_access_key_id = os.getenv(emr_aws_access_key_id_ev)
        self.emr_aws_secret_access_key = os.getenv(emr_aws_secret_access_key_ev)
        self.emr_aws_region = os.getenv(emr_aws_region_ev)

        self.config = {
            'aws_access_key_id': self.emr_aws_access_key_id,
            'aws_secret_access_key': self.emr_aws_secret_access_key,
            'aws_region': self.emr_aws_region
        }


class EMR:

    def __init__(self, **kwargs):

        self.config_object = EMRUtils(**kwargs)
        self.config = self.config_object.config
        self.cluster_config = None
        self._set_client()

    def _set_client(self):
        self.client = boto3.client('emr', region_name=self.config['aws_region'])
