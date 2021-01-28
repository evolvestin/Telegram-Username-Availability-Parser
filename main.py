import os
import stat
import shutil
import objects
from git.repo.base import Repo
# ========================================================================================================
stamp = objects.time_now()


def delete(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)
    return action, name, exc


Repo.clone_from('https://github.com/evolvestin/Telegram-Username-Availability-Parser/', 'temp')
for file_name in os.listdir('temp/master'):
    shutil.copy(f'temp/master/{file_name}', file_name)
shutil.rmtree('temp', onerror=delete)
# ========================================================================================================
print(f'Запуск оболочки за {objects.time_now() - stamp} секунды')


if __name__ == '__main__':
    from master import start
    start(stamp)
