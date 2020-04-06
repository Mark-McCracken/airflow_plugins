from airflow.operators import BaseOperator
from os import environ
from time import sleep
import requests
import xml.etree.ElementTree as ET


class RefreshDatasourceOperator(BaseOperator):
    auth_token = None
    site_id = None
    datasource = None
    job_id = None
    template_fields = ('sitename', 'hostname', 'datasource_name')
    ui_color = '#4287117'

    def __init__(self,
                 datasource_name,
                 poke_delay=30,
                 max_pokes=60,
                 hostname=None,
                 sitename=None,
                 *args,
                 **kwargs):
        super(RefreshDatasourceOperator, self).__init__(*args, **kwargs)
        if sitename is None:
            env_sitename = environ.get("TABLEAU_SITENAME")
            assert env_sitename is not None, "No sitename supplied, and no env variable called TABLEAU_SITENAME"
            self.sitename = env_sitename
        else:
            self.sitename = sitename
        if hostname is None:
            env_hostname = environ.get("TABLEAU_HOSTNAME")
            assert env_hostname is not None, "No hostname provided, and no env variable called TABLEAU_HOSTNAME"
            self.hostname = env_hostname
        else:
            self.hostname = hostname
        self.datasource_name = datasource_name
        self.username = environ.get("TABLEAU_USERNAME")
        self.password = environ.get("TABLEAU_PASSWORD")
        self.poke_delay = poke_delay
        self.max_pokes = max_pokes

    def execute(self, context):
        self.get_auth_token_and_site_id()
        self.get_datasource()
        self.refresh_datasource_now()
        self.wait_for_job_to_be_complete()

    def get_auth_token_and_site_id(self):
        url = f"{self.hostname}/api/3.4/auth/signin"
        payload = f"""
                    <tsRequest>
                        <credentials name="{self.username}" password="{self.password}">
                            <site contentUrl="{self.sitename}"/>
                        </credentials>
                    </tsRequest>
                """
        headers = {
            'Content-Type': "application/xml"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        root = ET.fromstring(response.text)
        credentials = root[0]
        self.auth_token = credentials.attrib['token']
        self.site_id = credentials[0].attrib['id']

    def get_datasource(self):
        page_number = 0
        page_size = 100

        def params():
            return f"?pageNumber={page_number}&pageSize={page_size}"

        def url():
            return f"{self.hostname}/api/3.4/sites/{self.site_id}/datasources{params()}"

        def pagination_has_more_pages(pagination_input):
            if page_number == 0:
                return True
            pagination_page_number = int(pagination_input.attrib['pageNumber'])
            total_available = int(pagination_input.attrib['totalAvailable'])
            return total_available > pagination_page_number * page_size

        def get_page_of_datasources():
            headers = {
                'Content-Type': "application/xml",
                'Authorization': f"Bearer {self.auth_token}"
            }
            response = requests.request("GET", url(), data="", headers=headers)
            root = ET.fromstring(response.text)
            print(f"Response: Get page of datasources status code: {response.status_code}."
                  f"XML response: {ET.dump(root)}")
            return root

        pagination, datasources = None, None
        datasource = None
        while datasource is None and pagination_has_more_pages(pagination):
            page_number = page_number + 1
            pagination, datasources = get_page_of_datasources()
            datasource = next((ds for ds in datasources if ds.attrib['name'] == self.datasource_name), None)
        assert datasource is not None, f"Could not find datasource called {self.datasource_name}"
        self.datasource = datasource

    def refresh_datasource_now(self):
        url = f"{self.hostname}/api/3.4/sites/{self.site_id}/datasources/{self.datasource.attrib['id']}/refresh"
        payload = "<tsRequest></tsRequest>"
        headers = {
            'Content-Type': "application/xml",
            'Authorization': f"Bearer {self.auth_token}"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        assert response.status_code == 202, f"Error refreshing the data source. Status Code: {response.status_code}"
        root = ET.fromstring(response.text)
        print(f"Response: Refresh datasource status code: {response.status_code}."
              f"XML response: {ET.dump(root)}")
        job = root[0]
        job_id = job.attrib['id']
        print(f"""Successfully ordered a refresh of [{self.datasource.attrib['type']}]
                    datasource [{self.datasource.attrib['name']}] with job ID [{job_id}]
                    You can view it here: [{self.datasource.attrib['webpageUrl']}]
                """)
        self.job_id = job_id

    def wait_for_job_to_be_complete(self):
        attempt_number = 0
        url = f"{self.hostname}/api/3.4/sites/{self.site_id}/jobs/{self.job_id}"
        headers = {
            'Content-Type': "application/xml",
            'Authorization': f"Bearer {self.auth_token}"
        }

        def get_status():
            response = requests.request("GET", url, data="", headers=headers)
            assert response.status_code == 200, f"Error checking the status of the job"
            root = ET.fromstring(response.text)
            print(f"Response: Get status of job status code: {response.status_code}."
                  f"XML response: {ET.dump(root)}")
            job_status = root[0]
            return job_status

        completed = False

        while (not completed) and (attempt_number < self.max_pokes):
            sleep(self.poke_delay)
            print(f"Attempt {attempt_number} to check for job completion of job id {self.job_id}")
            job = get_status()
            try:
                completed = int(job.attrib['progress']) == 100
                attempt_number += 1
            except KeyError:
                print("Error, progress key was not available in response")
                completed = False

        assert completed, f"Job timed out waiting for completion"
        print(f"Job ID [{self.job_id}] completed successfully")
