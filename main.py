import os
import re
import gspread
import objects
import _thread
import requests
from time import sleep
from GDrive import Drive
from copy import deepcopy
from itertools import product
from bs4 import BeautifulSoup
from string import ascii_lowercase
# ========================================================================================================
stamp1 = objects.time_now()
folder_id = ''


def variables_creation():
    global folder_id
    db = {}
    chunk = []
    chunks = []
    user_postfix = ''
    max_len_combs = 0
    file_name = 'symbol'
    prefix = str(min_length)
    objects.environmental_files()
    drive_client = Drive('google.json')
    drive_client_files = drive_client.files()
    if min_length != max_length:
        prefix += '-' + str(max_length)
    if os.environ.get('user_postfix'):
        user_postfix = os.environ['user_postfix']
        file_name += '+' + user_postfix

    file_name = prefix + file_name
    files = {file_name + postfix + '.txt': '' for postfix in ['_used', '_clear']}

    for file in drive_client_files:
        if file['name'] == 'telegram_usernames':
            folder_id = file['id']
        if file['name'] in list(files.keys()):
            files[file['name']] = file['id']
            drive_client.download_file(file['id'], file['name'])

    for file in files:
        if os.path.exists(file) is False:
            with open(file, 'w') as f:
                f.write('CLEAR_FILE_FILLER')
                f.close()

    for file in files:
        opened = open(file, 'r')
        text = re.sub('CLEAR_FILE_FILLER', '', opened.read())
        if text:
            db[file] = text.split(' ')
        else:
            db[file] = []
        opened.close()

    for length in range(min_length, max_length + 1):
        for value in product(ascii_lowercase, repeat=length):
            max_len_combs += 1
            chunk.append(''.join(value) + user_postfix)
            if len(chunk) == 50000:
                chunks.append(chunk)
                chunk.clear()
    if chunk:
        chunks.append(chunk)
    return db, files, chunks, file_name, max_len_combs


t_me = 'https://t.me/'
min_length = os.environ.get('min_length')
max_length = os.environ.get('max_length')
Auth = objects.AuthCentre(os.environ['TOKEN'])
ErrorAuth = objects.AuthCentre(os.environ['ERROR-TOKEN'])
if min_length and max_length:
    min_length, max_length = int(min_length), int(max_length)
    array_db, file_names, split_combinations, main_file, max_len_combinations = variables_creation()
    Auth.start_message(stamp1)
else:
    Auth.start_message(stamp1, '\nОшибка с переменными окружения.\n' + objects.bold('Бот выключен'))
    array_db, file_names, split_combinations, main_file, max_len_combinations = {}, {}, [], '', 1
# ========================================================================================================


def checking():
    global array_db
    if split_combinations:
        while True:
            try:
                for split in split_combinations:
                    for username in split:
                        if username not in array_db[main_file + '_used.txt']:
                            try:
                                response = requests.get(t_me + username)
                            except IndexError and Exception:
                                sleep(0.01)
                                try:
                                    response = requests.get(t_me + username)
                                except IndexError and Exception:
                                    response = None
                            if response:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                is_username_exist = soup.find('a', class_='tgme_action_button_new')
                                if is_username_exist is None:
                                    array_db[main_file + '_clear.txt'].append(username)
                                array_db[main_file + '_used.txt'].append(username)
            except IndexError and Exception:
                ErrorAuth.thread_exec()


def save_array_to_file(path, array):
    file = open(path, 'w')
    text = ' '.join(array)
    if text == '':
        text = 'CLEAR_FILE_FILLER'
    file.write(text)
    file.close()


def files_upload():
    global array_db, file_names
    drive_client = Drive('google.json')
    for file in file_names:
        if file_names[file] == '':
            response = drive_client.create_file(file, folder_id)
            file_names[file] = response['id']
            objects.printer(file + ' создан')
    while True:
        try:
            sleep(20)
            temp_db = deepcopy(array_db)
            for file_name in file_names:
                save_array_to_file(file_name, temp_db[file_name])
                try:
                    drive_client.update_file(file_names[file_name], file_name)
                except IndexError and Exception:
                    drive_client = Drive('google.json')
                    drive_client.update_file(file_names[file_name], file_name)

            if len(temp_db[main_file + '_used.txt']) == max_len_combinations:
                log_text = 'Цикл проверок доступности юзеров пройден. Записываем список свободных username'
                objects.printer(log_text + ' (' + main_file + ') в google')
                title = main_file + '/' + str(objects.time_now())
                file = open(main_file + '_clear.txt', 'r')
                text = file.read()
                file.close()
                chunks = [text[offset: offset + 50000] for offset in range(0, len(text), 50000)]
                spreadsheet = gspread.service_account('google.json').create(title, folder_id)
                worksheet = spreadsheet.sheet1
                worksheet.resize(rows=len(chunks), cols=1)
                spreadsheet.batch_update(objects.properties_json(worksheet.id, len(chunks), chunks[:200]))
                if len(chunks) > 200:
                    request_counter = 1
                    for loop in range(200, len(chunks), 200):
                        row_begin = loop + 1
                        row_end = row_begin + 200
                        if row_end > len(chunks):
                            row_end = len(chunks)
                        work_range = worksheet.range('A' + str(row_begin) + ':A' + str(row_end))
                        for row in range(row_begin, row_end + 1):
                            if row <= len(chunks):
                                work_range[row - loop - 1].value = chunks[row - 1]
                        worksheet.update_cells(work_range)
                        request_counter += 1
                        if request_counter == 50:
                            request_counter = 0
                            sleep(100)
                objects.printer('Успешно записана информация в google')
                for file_name in array_db:
                    array_db[file_name] = []
                    save_array_to_file(file_name, array_db[file_name])
                objects.printer('Запущен новый цикл проверок, файлы очищены')
        except IndexError and Exception:
            ErrorAuth.thread_exec()


if __name__ == '__main__':
    _thread.start_new_thread(files_upload, ())
    checking()
