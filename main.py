import os
import stat
import shutil
import objects
from datetime import datetime
from git.repo.base import Repo
# ========================================================================================================
stamp = datetime.now().timestamp()


def delete(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)
    return action, name, exc


def starting_print(timestamp):
    text = 'Оболочка запущена за '
    rounded = round(datetime.now().timestamp() - timestamp, 2)
    if 0 < rounded < 1:
        if len(str(rounded)) == 3:
            rounded = f'{rounded}0'
        text += f'{rounded} секунды'
    else:
        rounded = int(rounded)
        text += f'{rounded} секунд'
        if rounded < 10 or rounded > 20:
            if str(rounded)[-1] in ['1']:
                text += 'у'
            elif str(rounded)[-1] in ['2', '3', '4']:
                text += 'ы'
    objects.printer(text)


Repo.clone_from('https://github.com/evolvestin/Telegram-Username-Availability-Parser/', 'temp')
for file_name in os.listdir('temp/master'):
    shutil.copy(f'temp/master/{file_name}', file_name)
shutil.rmtree('temp', onerror=delete)
# ========================================================================================================
starting_print(stamp)


if __name__ == '__main__':
    from master import start
    start(int(stamp))
