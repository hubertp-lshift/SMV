#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""SMV DataSet Framework interface

This module defines the abstract classes which formed the SmvDataSet Framework for clients' projects
"""

from smv.smvpy import smvPy
from smv.utils import wrap_in_scala_option
import traceback

def SmvDQM():
    """Factory method for Scala SmvDQM"""
    return smvPy._jvm.SmvDQM.apply()

# Factory methods for DQM policies
def FailParserCountPolicy(threshold):
    return smvPy._jvm.FailParserCountPolicy(threshold)

def FailTotalRuleCountPolicy(threshold):
    return smvPy._jvm.FailTotalRuleCountPolicy(threshold)

def FailTotalFixCountPolicy(threshold):
    return smvPy._jvm.FailTotalFixCountPolicy(threshold)

def FailTotalRulePercentPolicy(threshold):
    return smvPy._jvm.FailTotalRulePercentPolicy(threshold * 1.0)

def FailTotalFixPercentPolicy(threshold):
    return smvPy._jvm.FailTotalFixPercentPolicy(threshold * 1.0)

# DQM task policies
def FailNone():
    return smvPy._jvm.DqmTaskPolicies.failNone()

def FailAny():
    return smvPy._jvm.DqmTaskPolicies.failAny()

def FailCount(threshold):
    return smvPy._jvm.FailCount(threshold)

def FailPercent(threshold):
    return smvPy._jvm.FailPercent(threshold * 1.0)

def DQMRule(rule, name = None, taskPolicy = None):
    task = taskPolicy or FailNone()
    name1 = wrap_in_scala_option(smvPy._jvm, name)
    return smvPy._jvm.DQMRule(rule._jc, name1, task)

def DQMFix(condition, fix, name = None, taskPolicy = None):
    task = taskPolicy or FailNone()
    name1 = wrap_in_scala_option(smvPy._jvm, name)
    return smvPy._jvm.DQMFix(condition._jc, fix._jc, name1, task)
