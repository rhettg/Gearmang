"""Module for command line functions.

Functions in here should make it easier to keep our command line tools somewhat standardized.
"""
import sys
import os
import logging
import argparse
import traceback

from email.mime.text import MIMEText

log = logging.getLogger(__name__)
main_log = logging.getLogger("__main__")

def setup_logging(options):
    if len(options.verbose) > 1:
        level = logging.DEBUG
    elif options.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING
    
    log_format = "%(asctime)s %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(level=level, format=log_format, stream=sys.stdout)

def default_arguments(parser):
    parser.add_argument('--verbose', '-v', dest='verbose', action='append_const', const=True, default=list())

#def report_crash(exc_info):
    #if not hasattr(config, 'crash_report'):
        #log.info("Skipping crash reporting")
        #return

    #msg_txt = """
#Host: %s
#PID: %d
#Version: %s

#""" % (os.uname()[1], os.getpid(), retrosift.version.version,)
    #msg_txt += "".join(traceback.format_exception(*exc_info))

    #message = MIMEText(msg_txt)
    #message['Subject'] = "Crash report from %s (%s)" % (sys.argv[0], repr(exc_info[1]))  

    #emailer.SystemEmailer().notify(message)


def main(configure_function, run_function):
    parser = argparse.ArgumentParser()
    
    default_arguments(parser)

    configure_function(parser)

    options = parser.parse_args()
    
    setup_logging(options)

    main_log.info("Starting")

    try:
        run_function(options)
    except Exception, e:
        #report_crash(sys.exc_info())
        log.exception("Exiting due to crash")
        sys.exit(1)
    
    main_log.info("Done.")
