import os
import time
import sys
import json
import logging
import traceback

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index, Column, Integer, String, DateTime, LargeBinary, Text

import gearman
import gearman.constants

import config
from retrosift import cmd

GEARMAN_TIMEOUT = 10.0

# How many times to report a crash on the same job
MAX_REPORT_ERRORS = 3

ERROR_DELAY_SECS = 5.0

# Task to indicate a job has failed and we don't know what to do with it
TASK_GEARMAN_FAIL = 'gearman_fail'

# Task to indicate a user is available for processing
TASK_USER_AVAILABLE = 'user_available'

# Task for check in the email account and examining the properties
TASK_VERIFY_EMAIL = 'verify_email'

# Task for checking a provider before it becomes a user (or independent of user-ness)
TASK_VERIFY_EMAIL_PROVIDER = 'verify_email_provider'

# Task that includes a list of dates and a user to download email for
TASK_FETCH_EMAIL = 'fetch_email'

# Process the specified email to extract images
TASK_DECODE_EMAIL = 'decode_email'

# Send processing complete email
TASK_SEND_EMAIL_COMPLETE = 'send_email_complete'

# Send share email
TASK_SEND_EMAIL_SHARE = 'send_email_share'

GEARMAN_HOSTS = config.gearmand['master']

log = logging.getLogger(__name__)

class GearmangError(Exception): pass


# We queue gearman job failures and stick them in a database. See failqueue_job() below as well as the
# gearman worker 'gearman_fail_worker.py'
Base = declarative_base()
class FailedGearmanJob(Base):
    __tablename__ = 'failed_gearman_job'

    id = Column(Integer, primary_key=True)
    time_created = Column(DateTime, nullable=False, default=sqlalchemy.sql.func.now())
    unique_key = Column(String(64), unique=True, nullable=False)
    task = Column(String(64), nullable=False)
    data = Column(LargeBinary)
    time_failed = Column(DateTime)
    exception = Column(Text)




class RetrosiftWorker(gearman.GearmanWorker):
    data_encoder = JSONDataEncoder
    def __init__(self, *args, **kwargs):
        super(RetrosiftWorker, self).__init__(*args, **kwargs)
        self.start_time = time.time()
        self.stop_on_error = False
    
    def after_poll(self, any_activity):
        if not self.has_job_locks():
            if self.stop_on_error:
                log.info("We had an error processing, time to exit.")
                return False

            if time.time() - self.start_time > 10:
                log.info("Running for %d seconds, exiting", time.time() - self.start_time)
                return False
        
        return True

    def has_job_locks(self):
        return bool(self.command_handler_holding_job_lock is None)

    def on_job_exception(self, current_job, exc_info):
        self.stop_on_error = True
        log.error("Failed processing %s task", current_job.task, exc_info=exc_info)

        # The default behavior for gearman is to just mark the job as
        # completed, I guess this is to prevent 'tasks of death' bringing down
        # all your workers. That behavior doesn't lead to providing very good
        # reliability though, as transient errors or bugs that could be fixed
        # can never be retried.

        # So what we're going to do is allow a couple of retries (in case it's something transient)
        # Afterwards, we're gonna stick the job in a fail queue where some other tooling can figure out
        # what to do with the job.

        current_job.data.setdefault('_job_errors', 0)
        current_job.data['_job_errors'] += 1

        if current_job.data['_job_errors'] < MAX_REPORT_ERRORS:
            log.warning("Failure iteration %d, trying again in %r", current_job.data['_job_errors'], ERROR_DELAY_SECS)
            time.sleep(ERROR_DELAY_SECS)

            # We're going make an attempt to re-queue the task so it's at least at
            # the end of the queue and doesn't impede tasks that are working
            # correctly (in the bug case)
            self.requeue_job(current_job)

        else:
            log.warning("Error iteration %d, offloading to fail queue", current_job.data['_job_errors'])
            self.failqueue_job(current_job, exc_info)

        # And we call the parent class, which will mark our current job as a
        # success Note that this is one of those reasons why it's important
        # that tasks can be handled multiple times, as there is always the
        # possiblity that we will fail before marking the original task
        # complete, meaning there will be two identical jobs in the queue.
        return super(RetrosiftWorker, self).on_job_exception(current_job, exc_info)

    def requeue_job(self, current_job):
        try:
            client = build_client()

            client.submit_job(
                              current_job.task, 
                              current_job.data, 
                              background=True, 
                              priority=gearman.constants.PRIORITY_LOW,
                              poll_timeout=GEARMAN_TIMEOUT,
                              )

        except Exception, e:
            log.exception("Failed to re-queue job")

    def failqueue_job(self, current_job, exc_info):
        fail_data = {
            'unique_key': current_job.unique,
            'task': current_job.task,
            'data': json.dumps(current_job.data),
            'time': time.time(),
            'exception': "".join(traceback.format_exception(*exc_info))
        }

        try:
            client = build_client()

            client.submit_job(
                              TASK_GEARMAN_FAIL,
                              fail_data, 
                              background=True, 
                              priority=gearman.constants.PRIORITY_LOW,
                              poll_timeout=GEARMAN_TIMEOUT,
                              )

        except Exception, e:
            log.exception("Failed to fail-queue job")

class RetrosiftClient(gearman.GearmanClient):
    data_encoder = JSONDataEncoder

    def submit_multiple_requests(self, *args, **kwargs):
        processed_requests = super(RetrosiftClient, self).submit_multiple_requests(*args, **kwargs)

        for req in processed_requests:
            if req.state in (gearman.constants.JOB_PENDING, gearman.constants.JOB_UNKNOWN):
                raise GearmangError("Job %s for queue %s didn't get created: %s" % (req.job.unique, req.job.task, req.state))

        return processed_requests

def build_worker():
    if DEBUG_worker:
        return DEBUG_worker

    gm_worker = RetrosiftWorker(GEARMAN_HOSTS)
    script_name = os.path.basename(sys.argv[0]).split('.')[0]
    gm_worker.set_client_id("%s-%s:%d" % (script_name, os.uname()[1], os.getpid()))

    return gm_worker

def build_client():
    if DEBUG_client:
        return DEBUG_client
    
    client = RetrosiftClient(GEARMAN_HOSTS)

    return client

DEBUG_client = None
DEBUG_worker = None

def DEBUG_set_client(client):
    global DEBUG_client

    DEBUG_client = client

def DEBUG_set_worker(worker):
    global DEBUG_worker

    DEBUG_worker = worker

def DEBUG_clear_client():
    global DEBUG_client
    DEBUG_client = None

def DEBUG_clear_worker():
    global DEBUG_worker
    DEBUG_worker = None
    
