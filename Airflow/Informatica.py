# coding: utf-8
# Python version 3.7

import logging
import os
import subprocess
import time
import re


# Журналы
journal_error = logging.getLogger('error.Informatica')
journal_log = logging.getLogger('journal.Informatica')

class Informatica:
    # Журналы
    journal_error = logging.getLogger('error.Informatica.Informatica')
    journal_log = logging.getLogger('journal.Informatica.Informatica')

    def __init__ (self, registry, infa_domains_file, pmcmd_path):
        self.o_registry = registry
        self.o_config = registry.get("o_config")
        self.infa_domains_file = infa_domains_file # E:\Informatica\10.2.0\clients\PowerCenterClient\domains.infa
        self.pmcmd_path = pmcmd_path # E:\Informatica\10.2.0\clients\PowerCenterClient\CommandLineUtilities\PC\server\bin\PmCmd

    def startWorkflow (self
        , server # whsdevpc951_is
        , domain # Domain_informatica1.corp.tander.ru
        , wait # True/False
        , repository #ANP
        , workflow #WF_GSS_TEST4
        , local_param_file #None or filepath
        , login=None #Используется для LDAP на боевой информатике
        , password=None #Используется для LDAP на боевой информатике
        ):

        return self._startWorkflow(
            server=server
            , domain=domain
            , wait=wait
            , repository=repository
            , workflow=workflow
            , local_param_file=local_param_file
            , login=login
            , password=password
        )[0]

    def _startWorkflow (self
        , server
        , domain
        , wait
        , repository
        , workflow
        , local_param_file
        , login=None
        , password=None
        ):

        try:
            local_param_file = f'-lpf "{local_param_file}"' if local_param_file is not None else ""
            auth_str = "-usd CORP.TANDER.RU -u {} -p {}".format(str(login), str(password)) if (login is not None and password is not None) else ""
            result = subprocess.run(f'set INFA_DOMAINS_FILE={self.infa_domains_file} && {self.pmcmd_path} startworkflow -sv {server} -d {domain} {local_param_file} {auth_str} {"-wait" if wait else ""} -f {repository} {workflow}', shell=True, stdout=subprocess.PIPE)

            if not wait: return True, 0, ''

            result_str= result.stdout.decode('utf-8').replace('\r', '')

            if result.returncode == 0:
                if re.search(r'INFO:.+succeeded', result_str):
                    return True, result.returncode, result_str
                else:
                    self.journal_log.error(f'Не удалось найти информацию о запуске потока в логах запуска - "{result_str}"!')
                    return False, result.returncode, result_str
            else:
                error_str = None

                if error_str is None:
                    find_error = re.search(r'ERROR:.+\n', result_str)
                    error_str = find_error.group(0) if find_error else None

                if error_str is None:
                    find_error = re.search(r'WARNING:.+\n', result_str)
                    error_str = find_error.group(0) if find_error else None

                if error_str is None:
                    error_str = 'Неизвестная ошибка'


                self.journal_log.error(f'Не удалось выполнить поток informatica {workflow}! {error_str}! Логи запуска - "{result_str}"!')

                return False, result.returncode, result_str
        except Exception as e:
            self.journal_error.error('Не удалось выполнить запуск потока informatica! Возникла ошибка "{}"'.format(str(e)), exc_info=True)
            return False, -1, ''

    def _validateGetStartWorkflow (self
        , server
        , domain
        , repository
        , workflow
        , login=None #Используется для LDAP на боевой информатике
        , password=None #Используется для LDAP на боевой информатике
        ):

        try:
            auth_str = "-usd CORP.TANDER.RU -u {} -p {}".format(str(login), str(password)) if (login is not None and password is not None) else ""
            result = subprocess.run(f'set INFA_DOMAINS_FILE={self.infa_domains_file} && {self.pmcmd_path} getworkflowdetails -sv {server} -d {domain} {auth_str} -f {repository} {workflow}', shell=True, stdout=subprocess.PIPE)

            if result.returncode == 0:
                result_str = result.stdout.decode('utf-8')

                if not re.search(r'Workflow run status: \[Running\]', result_str):
                    return True, 0
                else:
                    return False, 0

            else:
                status_arr = {
                      1  : 'ERROR: Cannot connect to Power Center server'
                    , 2  : 'ERROR: Workflow or folder does not exist'
                    , 3  : 'ERROR: An error occurred in starting or running the workflow'
                    , 4  : 'ERROR: Usage error'
                    , 5  : 'ERROR: Internal pmcmd error'
                    , 7  : 'ERROR: Invalid Username Password'
                    , 8  : 'ERROR: You do not have permission to perform this task'
                    , 9  : 'ERROR: Connection timed out'
                    , 13 : 'ERROR: Username environment variable not defined'
                    , 14 : 'ERROR: Password environment variable not defined'
                    , 15 : 'ERROR: Username environment variable missing'
                    , 16 : 'ERROR: Password environment variable missing'
                    , 17 : 'ERROR: Parameter file doesnot exist'
                    , 18 : 'ERROR: Initial value missing from parameter file'
                    , 20 : 'ERROR: Repository error occurred. Pls check repository server and database are running'
                    , 21 : 'ERROR: PowerCenter server shutting down'
                    , 22 : 'ERROR: Workflow not unique. Please enter folder name'
                    , 23 : 'ERROR: No data available'
                    , 24 : 'ERROR: Out of memory'
                    , 25 : 'ERROR: Command cancelled'
                }

                self.journal_log.warning(f'Не удалось проверить возможность запустить поток informatica {workflow}! {status_arr[result.returncode] if result.returncode in status_arr.keys() else "ERROR: Unknown"}')

                return False, result.returncode

        except Exception as e:
            self.journal_error.error('Не удалось выполнить проверку запуска потока informatica! Возникла ошибка "{}"'.format(str(e)), exc_info=True)
            return False, -1

    def startWorkflowGetStartValidate (self
        , server
        , domain
        , wait
        , repository
        , workflow
        , local_param_file

        , interval_sec # сколько секунд ожидания перед каждой итерацией
        , wait_max_sec # максимальное время ожидания в секундах
        
        , login=None #Используется для LDAP на боевой информатике
        , password=None #Используется для LDAP на боевой информатике
        ):

        try:
            start_check = time.time()
            while True:

                result, status = self._validateGetStartWorkflow(
                    server=server
                    , domain=domain
                    , repository=repository
                    , workflow=workflow
                    , login=login
                    , password=password
                )

                if not result and status != 0: return False
                if result:
                    result, status, result_str = self._startWorkflow(
                        server=server
                        , domain=domain
                        , wait=wait
                        , repository=repository
                        , workflow=workflow
                        , local_param_file=local_param_file
                        , login=login
                        , password=password
                    )

                    if result: return True
                    if not re.search(r'Could not start execution of this workflow because the current run on this Integration Service has not completed yet', result_str): return False

                if (round(time.time() - start_check)) > wait_max_sec:
                    self.journal_log.error(f'Превышен указанный лимит ожидания для запуска потока {str(wait_max_sec)} сек.!')
                    return False

                time.sleep(interval_sec)

        except Exception as e:
            self.journal_error.error('Не удалось выполнить запуск потока informatica! Возникла ошибка "{}"'.format(str(e)), exc_info=True)
            return False