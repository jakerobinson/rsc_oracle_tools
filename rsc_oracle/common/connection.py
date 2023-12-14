# Copyright 2020 Rubrik, Inc.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.


"""
Class that controls the connection with Rubrik RSC.
"""
import logging
import os
import json
import sys
import inspect
import time
import yaspin
from yaspin.spinners import Spinners
import requests
import urllib3
from gql import Client
from gql.transport.requests import RequestsHTTPTransport


class NoTraceBackWithLineNumber(Exception):
    """
    Limits Traceback on raise and only will raise object name and line number
    """
    def __init__(self, msg):
        try:
            ln = sys.exc_info()[-1].tb_lineno
        except AttributeError:
            ln = inspect.currentframe().f_back.f_lineno
        self.args = "{0.__name__} (line {1}): {2}".format(type(self), ln, msg),
        sys.exit(self)


class RbsOracleConnectionError(NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass


HTTP_ERRORS = {
    204: "No Content",
    400: "Bad request: An error occurred while fetching the data",
    401: "Authentication error: Please provide valid credentials",
    403: "Forbidden: Please provide valid credentials",
    409: "Conflict",
    404: "Resource not found",
    500: "The server encountered an error"
}


class RubrikConnection:
    """
    Creates a Rubrik RSC connection for API commands
    """
    def __init__(self, keyfile=None, insecure=False):
        self.logger = logging.getLogger(__name__ + '.RubrikConnection')
        self.logger.debug("Attempting to load json key file. This is the file downloaded when creating the service account in RSC. ")
        self.config = {
            'client_id': None,
            'client_secret': None,
            'name': None,
            'access_token_uri': None
        }
        if insecure:
            self.certificate_check = False
            urllib3.disable_warnings()
        else:
            self.certificate_check = True
        self.logger.debug("Using insecure connection: {}".format(insecure))
        self.logger.debug("Keyfile argument: {}".format(keyfile))
        if not keyfile:
            __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
            self.logger.debug("Checking for keyfile at {}.".format(__location__))
            __location__ = os.path.split(__location__)[0]
            __location__ = os.path.split(__location__)[0]
            __location__ = os.path.join(__location__, 'config')
            self.logger.debug("The config file location is {}.".format(__location__))

            keyfile = os.path.join(__location__, 'keyfile.json')
        if os.path.exists(keyfile):
            with open(keyfile) as config_file:
                self.config = json.load(config_file)
            for setting in self.config:
                if not (self.config[setting] and self.config[setting].strip()):
                    self.config[setting] = None
        else:
            self.logger.debug("No keyfile found at {}, trying environment variables".format(keyfile))
        if not self.config['client_id']:
            self.config['client_id'] = os.environ.get('rsc_client_id')
            if not self.config['client_id']:
                self.logger.debug("No rsc client id found in environment variables.")
        if not self.config['client_secret']:
            self.config['client_secret'] = os.environ.get('rsc_client_secret')
            if not self.config['client_secret']:
                self.logger.debug("No rsc client secret found in environment variables.")
        if not self.config['access_token_uri']:
            self.config['access_token_uri'] = os.environ.get('rsc_access_token_uri')
            if not self.config['access_token_uri']:
                self.logger.debug("No rsc token uri found in environment variables.")
        if not self.config['client_id'] or not self.config['client_secret'] or not self.config['access_token_uri']:
            raise RbsOracleConnectionError("No keyfile credentials found in keyfile or environmental variables")
        self.logger.debug("Instantiating RubrikConnection.")
        _payload = {
            "client_id": self.config['client_id'],
            "client_secret": self.config['client_secret'],
            "name": self.config['name']
        }
        _headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'Accept': 'application/json, text/plain'
        }
        self.logger.debug("Access_token_uri: {}".format(self.config['access_token_uri']))
        self.logger.debug("Headers: {}".format(_headers))
        self.logger.debug("Payload: {}".format(_payload))
        response = requests.post(
            self.config['access_token_uri'],
            verify=self.certificate_check,
            json=_payload,
            headers=_headers
        )
        if response.status_code != 200:
            if response.status_code in HTTP_ERRORS.keys():
                self.logger.warning(HTTP_ERRORS[response.status_code])
        response_json = response.json()
        if 'access_token' not in response_json:
            raise RbsOracleConnectionError("Unable to obtain access token from RSC.")
        self.logger.debug("Service Account session created and Access Token has been obtained...")
        self.access_token = response_json['access_token']
        self.headers = {'Content-Type': 'application/json;charset=UTF-8', 'Accept': 'application/json, text/plain',
                    'Authorization': 'Bearer ' + self.access_token}

    def delete_session(self):
        end_session_url = self.config['access_token_uri'].replace("client_token", "session")
        self.logger.debug("End session uri: {}".format(end_session_url))
        end_session_response = requests.delete(
            end_session_url,
            headers=self.headers
        )
        self.logger.debug("End session response: {}".format(end_session_response))
        if end_session_response.status_code == 204:
            self.logger.debug("Session deleted and token has been released...")
        else:
            self.logger.warning("Unable to delete session...")

    def graphql_query(self,query, query_variables=None):
        session_url = self.config['access_token_uri'].replace("client_token", "graphql")
        self.logger.debug("Session_URL: {}".format(session_url))
        transport = RequestsHTTPTransport(
            url=session_url,
            verify=self.certificate_check,
            retries=3,
            headers=self.headers
        )
        client = Client(transport=transport, fetch_schema_from_transport=False)
        result = client.execute(query, variable_values=query_variables)
        return result

    def async_requests_wait(self, requests_id, timeout):
        timeout_start = time.time()
        terminal_states = ['FAILED', 'CANCELED', 'SUCCEEDED']
        oracle_request = None
        while time.time() < timeout_start + (timeout * 60):
            oracle_request = self.graphql_query('internal', '/oracle/request/{}'.format(requests_id), timeout=self.cdm_timeout)
            if oracle_request['status'] in terminal_states:
                break
            with yaspin(Spinners.line, text='Request status: {}'.format(oracle_request['status'])):
                time.sleep(10)
        if oracle_request['status'] not in terminal_states:
            self.delete_session()
            raise RbsOracleConnectionError(
                "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current status: {0})  and the script will exit.".format(
                    oracle_request['status'], timeout))
        else:
            return oracle_request
