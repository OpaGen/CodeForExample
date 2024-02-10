import ldap3
import pythoncom
from pyad import pyad
import re

def domain_auth (login, password):
    try:
        conn = ldap3.Connection(ldap3.Server('adcore1.corp.tander.ru', port=389, use_ssl=False), user='corp\\'+str(login), password=str(password))
        return True if conn.bind() else False
    except Exception as e:
        return False

def get_domain_user (login):
    try:
        pythoncom.CoInitializeEx(0)
        return pyad.from_cn(login)
    except Exception as e:
        return False

def get_domain_user_fio (login):
    try:
        pythoncom.CoInitializeEx(0)
        ad_object = pyad.from_cn(login)
        if not ad_object:
            return None
            
        temp = ad_object.get_attribute('displayName')
        if not temp or len(temp)==0 or len(temp[0])==0:
            return None

        return temp[0]

    except Exception as e:
        return None

def get_name_from_ad_object (ad_object):
    try:
        if ad_object:
            temp = ad_object.get_attribute('givenName')
            return temp[0]
        else:
            return ''
    except Exception as e:
        return ''

def get_user_data (ad_object, ext_ad=False):
    ls = {
          'name'                   : 'ad_login'        # Логин в системе
        , 'displayName'            : 'ad_fio'          # ФИО
        , 'givenName'              : 'ad_firstname'    # Имя
        , 'sn'                     : 'ad_lastname'     # Фамилия
        , 'middleName'             : 'ad_middlename'   # Отчество
        , 'extensionAttribute13'   : 'ad_sex'          # Пол
        , 'title'                  : 'ad_position'     # Должность
        , 'mail'                   : 'ad_email'        # Почта
        , 'telephoneNumber'        : 'ad_telephone'    # Номер телефона
        , 'extensionAttribute8'    : 'ad_status'       # Статус

        , 'extensionAttribute1'    : 'ad_office'       # Офис
        , 'extensionAttribute2'    : 'ad_management'   # Дирекция
        # , 'extensionAttribute3'    : 'ad_ХЗ'           # ХЗ
        , 'extensionAttribute4'    : 'ad_department'   # Департамент
        , 'extensionAttribute5'    : 'ad_control'      # Управление
        , 'extensionAttribute6'    : 'ad_subdivision'  # Отдел
        , 'extensionAttribute7'    : 'ad_sector'       # Сектор
        , 'memberOf'               : 'ad_adgroups'     # АД группы
    }

    result = {}

    try:
        for i in ls:
            val = ad_object.get_attribute(i)
            if not val: continue

            if i == 'memberOf':
                ad_groups=[]

                for b in val:
                    res = re.findall(r'CN=[^,]+,', b)
                    if not res: continue

                    ad_group={}
                    ad_group['name'] = (res[0])[3:-1]
                    ad_group['description'] = ''
                    if ext_ad:
                        try:
                            gr_obj = pyad.from_cn(ad_group['name'])
                            if gr_obj:
                                group_description = gr_obj.get_attribute('description')
                                ad_group['description'] = group_description[0]
                        except Exception as e:
                            pass
                    ad_groups.append(ad_group)

                if ad_groups:
                    ad_groups.sort(key=lambda x: x['name'].upper())

                result[ls[i]] = ad_groups
            elif type(val) == list:
                result[ls[i]] = val[0]
            else:
                result[ls[i]] = val

    except Exception as e:
        pass

    return result