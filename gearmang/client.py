# -*- coding: utf-8 -*-
"""
Base Gearman client class with enhanced functionality:
  * Default JSON encoding/decoding
  * Handles failures to queue jobs based on timeout

:copyright: (c) 2012 by Rhett Garber
:license: ISC, see LICENSE for more details.
"""
import gearman
import gearman.constants

from . import encoder
from . import errors

class JobNotRequestedError(errors.Error):
    """Indicates the job was not successfully queued"""
    pass

class Client(gearman.GearmanClient):
    data_encoder = encoder.JSONDataEncoder

    def submit_multiple_requests(self, *args, **kwargs):
        processed_requests = super(Client, self).submit_multiple_requests(*args, **kwargs)

        # Check now for failures during the job queuing process. Requests may have been 
        # returned to us because of a timeout.
        for req in processed_requests:
            if req.state in (gearman.constants.JOB_PENDING, gearman.constants.JOB_UNKNOWN):
                raise JobNotRequestedError("Job %s for queue %s didn't get created: %s" % (req.job.unique, req.job.task, req.state))

        return processed_requests
