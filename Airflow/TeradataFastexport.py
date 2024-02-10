# coding: utf-8
# Python version 3.7

import logging
import os
import subprocess


# Журналы
journal_error = logging.getLogger('error.TeradataFastexport')
journal_log = logging.getLogger('journal.TeradataFastexport')

class TeradataFastexport:
    # Журналы
    journal_error = logging.getLogger('error.TeradataFastexport.TeradataFastexport')
    journal_log = logging.getLogger('journal.TeradataFastexport.TeradataFastexport')
    
    def __init__ (self, registry):
        self.o_registry = registry
        self.o_config = registry.get("o_config")
        
        self.fastexport_exe = '"' + os.path.join(self.o_config.get("DIR_KERNEL"), 'system', 'db', 'teradata', 'tpt', 'fexp.exe') + '"'
        
    def start (self, file_fl):
        try:
            result = subprocess.call('{} < "{}"'.format(self.fastexport_exe, file_fl), shell=True)
            if result is not None and result == 0:
                return True
            else:
                return False
        except Exception as e:
            self.journal_error.error('Не удалось выполнить выгрузку данных через FEXP! Возникла ошибка "{}"'.format(str(e)), exc_info=True)
            return False

        return True
