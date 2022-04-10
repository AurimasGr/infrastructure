import time
import requests
import json


class Session:

    def __init__(self, cluster_dns):
        self.dns = cluster_dns
        self.id = ''

        self.JSON_HEADER = {'Content-Type': 'application/json'}

        response = requests.post(self.url, data=json.dumps({'kind': 'pyspark'}), headers=self.JSON_HEADER)
        self.id = response.json()['id']

        print('Establishing a Spark session...', flush=True)
        status = None
        while status != 'idle':
            # print("sleeping for 3...", flush=True)
            time.sleep(3)
            status_response = requests.get(self.url, headers=self.JSON_HEADER)
            status = status_response.json()['state']
            # print(status, flush=True)
        print(f'...session ({self.id}) is ready.', flush=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    @property
    def url(self):
        return f'http://{self.dns}:8998/sessions/{self.id}'

    def terminate(self):
        print(f'Terminating the session ({self.id}).', flush=True)
        requests.delete(self.url, headers=self.JSON_HEADER)

    def submit(self, code, follow=True):
        endpoint = f'{self.url}/statements'
        response = requests.post(endpoint, data=json.dumps({'code': code}), headers=self.JSON_HEADER)
        endpoint = f"{endpoint}/{response.json()['id']}"
        if follow:
            state = None
            while state != 'available':
                time.sleep(3)
                response_json = requests.get(endpoint, headers=self.JSON_HEADER).json()
                state = response_json['state']

            if response_json['output']['status'] == 'error':
                print(f"Statement exception: {response_json['output']['evalue']}")
                for trace in response_json['output']['traceback']:
                    print(trace)
            output = response_json['output']['data'].get('text/plain')
            if output is not None:
                print(output, flush=True)
        return response
