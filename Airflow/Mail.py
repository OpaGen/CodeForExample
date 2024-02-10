# coding: utf-8
# Python version 3.7

import logging
import os
from shutil import copyfile
from ac_email import sendmail as ac_email_sendmail

# Журналы
journal_error = logging.getLogger('error.Mail')
journal_log = logging.getLogger('journal.Mail')

class Mail:
    # Журналы
    journal_error = logging.getLogger('error.Mail.Mail')
    journal_log = logging.getLogger('journal.Mail.Mail')

    def __init__ (self, registry):
        self.o_registry = registry
        self.o_config = registry.get("o_config")
        self.o_journal_error_controller = registry.get("o_journal_error_controller")
        self.o_journal_log_controller = registry.get("o_journal_log_controller")

    def sendMail (self, mail_from, mail_to, mail_cc = None, smtp_server = 'email.magnit.ru', smtp_port = 25, subject = '', msg = '', header = None, attachment = None, mail_importance = False):
        result = ac_email_sendmail(mail_from=mail_from,
            mail_to=mail_to,
            mail_cc=mail_cc,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            subject=subject,
            message=msg,
            attachment=attachment,
            mail_importance=mail_importance,
            is_template=True,
            header=header,
            is_filter_ad=True
        )
        
        if result:
            self.journal_log.debug("Было отправлено письмо")
            return True
        else:
            self.journal_error.warning("Не удалось отправить письмо!")
            return False

    def sendMailWithJournalFiles (self, mail_from, mail_to, mail_cc = None, smtp_server = 'email.magnit.ru', smtp_port = 25, subject = '', msg = '', header = None, mail_importance = False):
        # Делаем слепки журналов для отправки в письме
        attachment = []

        journal_log_temp = None
        journal_error_temp = None

        if self.o_journal_error_controller.issetJournal():
            try:
                journal_log_temp = os.path.join(self.o_config.get("DIR_TEMP"), self.o_journal_error_controller.getFilename())
                self.o_journal_error_controller.acquire()
                copyfile(self.o_journal_error_controller.getFilePath(), journal_log_temp)
                self.o_journal_error_controller.release()
                attachment.append(journal_log_temp)
            except Exception as e:
                pass

        if self.o_journal_log_controller.issetJournal():
            try:
                journal_error_temp = os.path.join(self.o_config.get("DIR_TEMP"), self.o_journal_log_controller.getFilename())
                self.o_journal_log_controller.acquire()
                copyfile(self.o_journal_log_controller.getFilePath(), journal_error_temp)
                self.o_journal_log_controller.release()
                attachment.append(journal_error_temp)
            except Exception as e:
                pass

        # Отправляем сообщение
        result = ac_email_sendmail(mail_from=mail_from,
            mail_to=mail_to,
            mail_cc=mail_cc,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            subject=subject,
            message=msg,
            attachment=attachment,
            mail_importance=mail_importance,
            is_template=True,
            header=header,
            is_filter_ad=True
        )

        # Удаляем временные файлы журналов
        if journal_log_temp is not None:
            try:
                os.remove(journal_log_temp)
            except Exception as e:
                pass

        if journal_error_temp is not None:
            try:
                os.remove(journal_error_temp)
            except Exception as e:
                pass

        if result:
            self.journal_log.debug("Было отправлено письмо")
            return True
        else:
            self.journal_error.warning("Не удалось отправить письмо!")
            return False