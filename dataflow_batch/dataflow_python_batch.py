from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.models.baseoperator import BaseOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.gcp_dataflow_hook import GoogleCloudBaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from googleapiclient.discovery import build
from airflow.utils.decorators import apply_defaults
import re
import select
import uuid
import subprocess

DEFAULT_DATAFLOW_LOCATION = "europe-west1"


class _DataflowJob(GoogleCloudBaseHook):
    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 project=None,
                 location=DEFAULT_DATAFLOW_LOCATION,
                 job_name=None, num_retries=0,
                 *args, **kwargs):
        super(_DataflowJob, self).__init__(*args, **kwargs)
        self._project = project
        self._job_location = location
        self._job_name = job_name
        self._num_retries = num_retries
        self._dataflow = self.get_conn()
        self._job_id = self._get_job_id_from_name()
        self._job = self._get_job()

    def get_conn(self):
        """
        Returns a Google Cloud Dataflow service object.
        """
        http_authorized = self._authorize()
        return build('dataflow', 'v1b3', http=http_authorized, cache_discovery=False)

    def _get_job_id_from_name(self):
        jobs = self._dataflow.projects().locations().jobs().list(
            projectId=self._project,
            location=self._job_location
        ).execute(num_retries=self._num_retries)
        for job in jobs['jobs']:
            if job['name'].lower() == self._job_name.lower():
                self._job_id = job['id']
                return job['id']
        return None

    def _get_job(self):
        self.log.info(self._job_id)
        job = self._dataflow.projects().locations().jobs().get(
            projectId=self._project,
            location=self._job_location,
            jobId=self._job_id).execute(num_retries=self._num_retries)
        self.log.info(job)
        return job

    def check_if_done(self):
        if not self._job:
            raise Exception("Job not defined")
        if 'currentState' not in self._job:
            raise Exception("Could not determine current State")

        name = self._job['name']
        state = self._job['currentState']
        if 'JOB_STATE_DONE' == state:
            return True
        elif 'JOB_STATE_RUNNING' == state:
            return False
        elif 'JOB_STATE_PENDING' == state:
            return False
        elif 'JOB_STATE_QUEUED' == state:
            return False
        elif 'JOB_STATE_FAILED' == state:
            raise Exception(f"Google Cloud Dataflow job {name} has failed.")
        elif 'JOB_STATE_CANCELLED' == state:
            raise Exception(f"Google Cloud Dataflow job {name} was cancelled.")
        else:
            self.log.debug(str(self._job))
            raise Exception(f"Google Cloud Dataflow job {name} was unknown state: {state}")

    def get(self):
        return self._job


class _Dataflow(LoggingMixin):
    def __init__(self, cmd):
        self.log.info("Running command: %s", ' '.join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True)

    def _line(self, fd):
        if fd == self._proc.stderr.fileno():
            line = b''.join(self._proc.stderr.readlines())
            if line:
                self.log.warning(line[:-1])
            return line
        if fd == self._proc.stdout.fileno():
            line = b''.join(self._proc.stdout.readlines())
            if line:
                self.log.info(line[:-1])
            return line

    @staticmethod
    def _extract_job(line):
        # Job id info: https://goo.gl/SE29y9.
        job_id_pattern = re.compile(
            br'.*console.cloud.google.com/dataflow.*/jobs/([a-z|0-9|A-Z|\-|\_]+).*')
        matched_job = job_id_pattern.search(line or '')
        if matched_job:
            return matched_job.group(1).decode()

    def wait_for_command_done(self):
        reads = [self._proc.stderr.fileno(), self._proc.stdout.fileno()]
        self.log.info("Start waiting for DataFlow process to complete.")
        for line in self._proc.stdout.readlines():
            self.log.info(line)
        for line in self._proc.stderr.readlines():
            self.log.info(line)
        job_id = None
        # Make sure logs are processed regardless whether the subprocess is
        # terminated.
        process_ends = False
        while True:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    line = self._line(fd)
                    if line:
                        job_id = job_id or self._extract_job(line)
            else:
                self.log.info("Waiting for DataFlow process to complete.")
            if process_ends:
                break
            if self._proc.poll() is not None:
                # Mark process completion but allows its outputs to be consumed.
                process_ends = True
        if self._proc.returncode != 0:
            self.log.info(reads)
            raise Exception(f"DataFlow failed with return code {self._proc.returncode}")
        return job_id


class DataFlowHook(GoogleCloudBaseHook):
    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poll_sleep=10):
        self.poll_sleep = poll_sleep
        super(DataFlowHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Dataflow service object.
        """
        http_authorized = self._authorize()
        return build(
            'dataflow', 'v1b3', http=http_authorized, cache_discovery=False)

    @GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
    def _start_dataflow(self, variables, name, command_prefix, label_formatter):
        variables = self._set_variables(variables)
        cmd = command_prefix + self._build_cmd(variables, label_formatter)
        job_id = _Dataflow(cmd).wait_for_command_done()
        self.log.info(f"Dataflow job ID is {job_id}")
        return job_id

    @staticmethod
    def _set_variables(variables):
        if variables['project'] is None:
            raise Exception('Project not specified')
        if 'region' not in variables.keys():
            variables['region'] = DEFAULT_DATAFLOW_LOCATION
        return variables

    def start_template_dataflow(self, job_name, variables, parameters, dataflow_template,
                                append_job_name=True):
        variables = self._set_variables(variables)
        name = self._build_dataflow_job_name(job_name, append_job_name)
        self._start_template_dataflow(
            name, variables, parameters, dataflow_template)

    def start_python_dataflow(self, job_name, variables, dataflow, py_options,
                              append_job_name=True):
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['job_name'] = name

        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value)
                    for key, value in labels_dict.items()]
        self._start_dataflow(variables, name, ["python3"] + py_options + [dataflow],
                             label_formatter)
        return name

    @staticmethod
    def _build_dataflow_job_name(job_name, append_job_name=True):
        base_job_name = str(job_name).replace('_', '-')

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                'Invalid job_name ({}); the name must consist of'
                'only the characters [-a-z0-9], starting with a '
                'letter and ending with a letter or number '.format(base_job_name))

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @staticmethod
    def _build_cmd(variables, label_formatter):
        command = ["--runner=DataflowRunner"]
        if variables is not None:
            for attr, value in variables.items():
                if attr == 'labels':
                    command += label_formatter(value)
                elif value is None or value.__len__() < 1:
                    command.append("--" + attr)
                else:
                    command.append("--" + attr + "=" + value)
        return command

    def _start_template_dataflow(self, name, variables, parameters,
                                 dataflow_template):
        #  Builds RuntimeEnvironment from variables dictionary # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment = {}
        for key in ['numWorkers', 'maxWorkers', 'zone', 'serviceAccountEmail',
                    'tempLocation', 'bypassTempDirValidation', 'machineType',
                    'ipConfiguration', # value should be 'WORKER_IP_PRIVATE' for private IP address space, useful for VPN connections / NAT
                    'additionalExperiments', 'network', 'subnetwork', 'additionalUserLabels']:
            if key in variables:
                environment.update({key: variables[key]})
        body = {"jobName": name,
                "parameters": parameters,
                "environment": environment}
        service = self.get_conn()
        request = service.projects().locations().templates().launch(
            projectId=variables['project'],
            location=variables['region'],
            gcsPath=dataflow_template,
            body=body
        )
        response = request.execute(num_retries=self.num_retries)
        variables = self._set_variables(variables)
        job = _DataflowJob(self.get_conn(), variables['project'], name, variables['region'],
                     self.poll_sleep, num_retries=self.num_retries).wait_for_done()
        return response



class StartDataflowPythonBatchJobOperator(BaseOperator):
    """
    Launching Cloud Dataflow jobs written in python. Note that both
    dataflow_default_options and options will be merged to specify pipeline
    execution parameter, and dataflow_default_options is expected to save
    high-level options, for instances, project and zone information, which
    apply to all dataflow operators in the DAG.

    .. seealso::
        For more detail on job submission have a look at the reference:
        https://cloud.google.com/dataflow/pipelines/specifying-exec-params

    :param py_file: Reference to the python dataflow pipeline file.py, e.g.,
        /some/local/file/path/to/your/python/pipeline/file. (templated)
    :type py_file: str
    :param job_name: The 'job_name' to use when executing the DataFlow job
        (templated). This ends up being set in the pipeline options, so any entry
        with key ``'jobName'`` or ``'job_name'`` in ``options`` will be overwritten.
    :type job_name: str
    :param py_options: Additional python options, e.g., ["-m", "-v"].
    :type pyt_options: list[str]
    :param dataflow_default_options: Map of default job options.
    :type dataflow_default_options: dict
    :param options: Map of job specific options.
    :type options: dict
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide  delegation enabled.
    :type delegate_to: str
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :type poll_sleep: int
    """
    template_fields = ['options', 'dataflow_default_options', 'job_name', 'py_file']

    @apply_defaults
    def __init__(
            self,
            py_file,
            job_name='{{task.task_id}}',
            py_options=None,
            dataflow_default_options=None,
            options=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            poll_sleep=10,
            *args,
            **kwargs):

        super(StartDataflowPythonBatchJobOperator, self).__init__(*args, **kwargs)

        self.py_file = py_file
        self.job_name = job_name
        self.py_options = py_options or []
        self.dataflow_default_options = dataflow_default_options or {}
        self.options = options or {}
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.poll_sleep = poll_sleep

    def execute(self, context):
        """Execute the python dataflow job."""
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        # Convert argument names from lowerCamelCase to snake case.
        camel_to_snake = lambda name: re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        job_name = hook.start_python_dataflow(
            self.job_name, formatted_options,
            self.py_file, self.py_options)
        return job_name


class DataflowJobCompleteSensor(BaseSensorOperator):
    template_fields = ('job_name',)

    @apply_defaults
    def __init__(self,
                 job_name,
                 project,
                 location=None,
                 gcp_conn_id=None,
                 *args, **kwargs):
        super(DataflowJobCompleteSensor, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.project = project
        self.gcp_conn_id = gcp_conn_id
        self.location = location

    def poke(self, context):
        job = _DataflowJob(self.gcp_conn_id, project=self.project, job_name=self.job_name)
        return job.check_if_done()
