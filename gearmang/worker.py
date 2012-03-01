# -*- coding: utf-8 -*-
"""
Base Gearman worker class with enhanced functionality:
  * Default JSON encoding/decoding
  * Sophisticated failure and retry handling

:copyright: (c) 2012 by Rhett Garber
:license: ISC, see LICENSE for more details.
"""
import time
import logging
import traceback
import json

import gearman
import gearman.constants

from . import encoder
from . import client

log = logging.getLogger(__name__)

# Task to indicate a job has failed and we don't know what to do with it
TASK_GEARMAN_FAIL = 'gearman_fail'

# How many times to report a crash on the same job
MAX_REPORT_ERRORS = 3

ERROR_DELAY_SECS = 5.0

GEARMAN_TIMEOUT = 10.0

class Worker(gearman.GearmanWorker):
    data_encoder = encoder.JSONDataEncoder
    def __init__(self, host_list=None):
        super(Worker, self).__init__(host_list=host_list)
        self.start_time = time.time()
        self.stop_on_error = False

        # Our worker is potentially going to need a client to requeue failed jobs
        self._client = client.Client(host_list)
    
    def after_poll(self, any_activity):
        # Note that we only process our after poll activity if we are not holding job locks.
        # This means we are inbetween jobs.
        # Very important if you intend to do something like exit the process after running
        # as you don't want to do this before your job completion has been marked by the server.
        # See also https://github.com/Yelp/python-gearman/pull/16
        if not self.has_job_locks():
            if self.stop_on_error:
                log.info("We had an error processing, time to exit.")
                return False

            if time.time() - self.start_time > 10:
                log.info("Running for %d seconds, exiting", time.time() - self.start_time)
                return False
        
        return True

    def has_job_locks(self):
        "Returns true if the worker is currently processing a job"
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
        return super(Worker, self).on_job_exception(current_job, exc_info)

    def requeue_job(self, current_job):
        """Re-queue the specified job, putting it at the end of the queue to make another attempt later"""
        try:
            self._client.submit_job(
                              current_job.task, 
                              current_job.data, 
                              background=True, 
                              priority=gearman.constants.PRIORITY_LOW,
                              poll_timeout=GEARMAN_TIMEOUT,
                              )

            return True
        except Exception, e:
            log.exception("Failed to re-queue job")
            return False

    def failqueue_job(self, current_job, exc_info):
        """Queue the failed job into the 'fail queue' meaning some external process is going to handle it now."""
        fail_data = {
            'unique_key': current_job.unique,
            'task': current_job.task,
            'data': json.dumps(current_job.data), # TODO: Should probably make use of our worker's encoder
            'time': time.time(),
            'exception': "".join(traceback.format_exception(*exc_info))
        }

        try:
            self._client.submit_job(
                              TASK_GEARMAN_FAIL,
                              fail_data, 
                              background=True, 
                              priority=gearman.constants.PRIORITY_LOW,
                              poll_timeout=GEARMAN_TIMEOUT,
                              )

            return True
        except Exception, e:
            log.exception("Failed to fail-queue job")
            return False
