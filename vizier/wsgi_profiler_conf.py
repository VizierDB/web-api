# Copyright (C) 2018 New York University
#                    University at Buffalo,
#                    Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cProfile
import pstats
import StringIO
import logging
import os
import time

PROFILE_LIMIT = int(os.environ.get("PROFILE_LIMIT", 30))
PROFILER = bool(int(os.environ.get("PROFILER", 1)))

print """
# ** USAGE:
$ PROFILE_LIMIT=100 gunicorn -c ./wsgi_profiler_conf.py wsgi
# ** TIME MEASUREMENTS ONLY:
$ PROFILER=0 gunicorn -c ./wsgi_profiler_conf.py wsgi
"""


def profiler_enable(worker, req):
    worker.profile = cProfile.Profile()
    worker.profile.enable()
    worker.log.info("PROFILING %d: %s" % (worker.pid, req.uri))


def profiler_summary(worker, req):
    s = StringIO.StringIO()
    worker.profile.disable()
    ps = pstats.Stats(worker.profile, stream=s).sort_stats('time', 'cumulative')
    ps.print_stats(PROFILE_LIMIT)

    logging.error("\n[%d] [INFO] [%s] URI %s" % (worker.pid, req.method, req.uri))
    logging.error("[%d] [INFO] %s" % (worker.pid, unicode(s.getvalue())))


def pre_request(worker, req):
    worker.start_time = time.time()
    with open("/usr/local/source/web-api/.vizierdb/logs/timing.log", "a") as f:
            f.write('api, ' + req.method + ':' + str(req.path) + ', ' + str(req.query) + ', start, '+str(worker.start_time)+ "\n")
    if PROFILER is True:
        profiler_enable(worker, req)


def post_request(worker, req, *args):
    end_time = time.time()
    total_time = end_time - worker.start_time
    with open("/usr/local/source/web-api/.vizierdb/logs/timing.log", "a") as f:
            f.write('api, ' + req.method + ':' + str(req.path) + ', ' + str(req.query) + ', end, ' + str(end_time) + "\n")
            f.write('api, ' + req.method + ':' + str(req.path) + ', ' + str(req.query) + ', duration, ' + str(total_time*1000) + "\n")
    logging.error("\n[%d] [INFO] [%s] Load Time: %.3fs\n" % (
        worker.pid, req.method, total_time))
    if PROFILER is True:
        profiler_summary(worker, req)
