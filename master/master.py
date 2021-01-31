import os
import re
import gspread
import objects
import heroku3
import _thread
from time import sleep
from GDrive import Drive
from itertools import product
from string import ascii_lowercase, ascii_uppercase
# ========================================================================================================


def variables_creation():
    global master
    objects.environmental_files()
    client = gspread.service_account('google.json')
    sheet = client.open_by_key(master['sheet_id']).worksheet('main')
    drive_client = Drive('google.json')
    resources = sheet.get('A1:Z50000')
    keys = resources.pop(0)
    scripts = []
    for row in resources:
        if len(row) == len(keys):
            worker = {'update': 0, 'server': '1'}
            for i in range(0, len(keys)):
                worker[keys[i]] = row[i]
            scripts.append(worker)

    for worker in scripts:
        split = {'1': {}, '2': {}}
        for key in worker:
            if key[-1] in ['1', '2']:
                split[key[-1]][key[:-1].strip()] = worker[key]
        if os.environ.get('local') is None:
            for key in split:
                connection = heroku3.from_key(split[key]['HEROKU API'])
                if connection.account().email != split[key]['EMAIL']:
                    worker[f'EMAIL {key}'] = connection.account().email
                    worker['update'] = 1
                for app in connection.apps():
                    if len(app.dynos()) > 0:
                        worker['server'] = key
                    if app.name != split[key]['THREAD']:
                        worker[f'THREAD {key}'] = app.name
                        worker['update'] = 1

    for folder in drive_client.files(only_folders=True):
        if folder['name'] == 'temp':
            master['temp_id'] = folder['id']
        elif folder['name'] == 'telegram_usernames':
            master['main_id'] = folder['id']
        elif folder['name'].startswith('logs'):
            if master['logs_created_time'] <= folder['createdTime']:
                master['logs_number'] = int(re.sub(r'\D', '', folder['name']))
                master['logs_created_time'] = folder['createdTime']
                master['logs_id'] = folder['id']

    for length in range(3, 5 + 1):
        for value in product(f"{ascii_lowercase}_", repeat=length):
            username = ''.join(value)
            if username.startswith('_') is False and re.search('__', username) is None \
                    and username[:1].isdigit() is False:
                if length in [3, 4]:
                    username += 'bot'
                if username.endswith('_') is False:
                    master['max_users_count'] += 1

    print(scripts)
    print(master)
    return keys, sheet, scripts


# ========================================================================================================


def google_update():
    global worksheet, workers
    while True:
        try:
            sleep(20)
            try:
                resources = worksheet.get('A1:Z50000')
            except IndexError and Exception:
                client = gspread.service_account('google.json')
                worksheet = client.open_by_key(master['sheet_id']).worksheet('main')
                resources = worksheet.get('A1:Z50000')
            for worker in workers:
                if worker['update'] == 1:
                    row_index = len(resources) + 1
                    for row in resources:
                        if worker['PREFIX'] in row:
                            row_index = resources.index(row) + 1
                    values_indexes = range(0, len(google_keys))
                    client = gspread.service_account('google.json')
                    connection = heroku3.from_key(worker[f"HEROKU API {worker['server']}"])
                    google_range = f'A{row_index}:{ascii_uppercase[values_indexes[-1]]}{row_index}'
                    worksheet = client.open_by_key(master['sheet_id']).worksheet('main')
                    work_range = worksheet.range(google_range)
                    for i in values_indexes:
                        work_range[i].value = worker[google_keys[i]]
                    worksheet.update_cells(work_range)
                    for app in connection.apps():
                        app.restart()
                    worker['update'] = 0
                    sleep(5)

            resources.pop(0)
            workers_count = 0
            ended_workers_count = 0
            for google_worker in resources:
                if len(google_worker) == len(google_keys):
                    workers_count += 1
                    if google_worker[google_keys.index('PROGRESS')] == '✅':
                        ended_workers_count += 1
            if str(workers_count) != master['workers_count']:
                for worker in workers:
                    connection = heroku3.from_key(worker[f"HEROKU API {worker['server']}"])
                    for app in connection.apps():
                        app.restart()
                for api in [master['another_api'], master['api']]:
                    connection = heroku3.from_key(api)
                    for app in connection.apps():
                        config = app.config()
                        config['workers_count'] = str(workers_count)
            if workers_count == ended_workers_count:
                drive_client = Drive('google.json')
                logs_db = {'clear': [], 'used_count': 0}
                objects.printer('Цикл проверок доступности юзеров пройден. '
                                'Записываем список свободных username в google')

                if len(drive_client.files(parents=master['logs_id'])) >= 400:
                    folder = drive_client.create_folder(f"logs-{master['logs_number'] + 1}", master['main_id'])
                    master['logs_created_time'] = folder['createdTime']
                    master['logs_id'] = folder['id']
                    master['logs_number'] += 1

                for file in drive_client.files(parents=master['temp_id']):
                    drive_client.download_file(file['id'], file['name'])
                    local_file = open(file['name'], 'r')
                    array = local_file.read().split(' ')
                    if 'clear' in file['name']:
                        logs_db['clear'].extend(array)
                    else:
                        logs_db['used_count'] += len(array)
                    local_file.close()
                    os.remove(file['name'])

                if logs_db['used_count'] >= master['max_users_count']:
                    step = 50
                    array = ' '.join(logs_db['clear'])
                    chunks = [array[offset: offset + 50000] for offset in range(0, len(array), 50000)]
                    spreadsheet = gspread.service_account('google.json').create('logs', master['logs_id'])
                    logs_worksheet = spreadsheet.sheet1
                    logs_worksheet.resize(rows=len(chunks), cols=1)
                    spreadsheet.batch_update(objects.properties_json(logs_worksheet.id, len(chunks), chunks[:step]))
                    if len(chunks) > step:
                        request_counter = 1
                        for loop in range(step, len(chunks), step):
                            row_begin = loop + 1
                            row_end = row_begin + step
                            if row_end > len(chunks):
                                row_end = len(chunks)
                            work_range = logs_worksheet.range(f'A{row_begin}:A{row_end}')
                            for row in range(row_begin, row_end + 1):
                                if row <= len(chunks):
                                    work_range[row - loop - 1].value = chunks[row - 1]
                            logs_worksheet.update_cells(work_range)
                            request_counter += 1
                            if request_counter == 50:
                                request_counter = 0
                                sleep(100)
                    log_text = 'Успешно записана информация в telegram_usernames/' \
                               f"logs-{master['logs_number']}/"
                    objects.printer(log_text)
                    for worker in workers:
                        worker['update'] = 1
                        worker['status'] = '🅰️'
                    logs_db.clear()
                    sleep(100)
                else:
                    log_text = objects.bold('Нарушена целостность массива\n(все workers закончили работу):\n') + \
                        f"Необходимая длина массива проверок = {objects.code(master['max_users_count'])}\n" \
                        f"Длина полученного массива = {objects.code(logs_db['used_count'])}"
                    objects.printer(re.sub('<.*?>', '', log_text))
                    ErrorAuth.send_dev_message(log_text, tag=None)
                    logs_db.clear()
                    sleep(10800)
        except IndexError and Exception:
            ErrorAuth.thread_exec()


def checking():
    global worksheet
    while True:
        try:
            sleep(20)
        except IndexError and Exception:
            ErrorAuth.thread_exec()


master = {
    'main_id': '',
    'temp_id': '',
    'logs_id': '',
    'logs_number': 0,
    'max_users_count': 0,
    'logs_created_time': 0,
    'api': os.environ.get('api'),
    'another_api': os.environ.get('another_api'),
    'workers_count': os.environ.get('workers_count'),
    'sheet_id': '1OdAyu4zUTgww6AXaCeuJwT6OJzokWqPxPpjx1zbKvKQ'}

t_me = 'https://t.me/'
Auth = objects.AuthCentre(os.environ['DEV-TOKEN'])
google_keys, worksheet, workers = variables_creation()
ErrorAuth = objects.AuthCentre(os.environ['ERROR-TOKEN'])
# ========================================================================================================


def start(stamp):
    for key in master:
        if master[key] in ['', 0, None]:
            break
    else:
        _thread.start_new_thread(google_update, ())
        if os.environ.get('local') is None:
            Auth.start_message(stamp)
        checking()

    ErrorAuth.start_message(stamp, f"\nОшибка с переменными окружения.\n{objects.bold('Бот выключен')}")


if os.environ.get('local'):
    start(objects.time_now())
