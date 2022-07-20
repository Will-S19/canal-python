import sys
import os
import django

pwd = os.path.dirname(os.path.realpath(__file__))

sys.path.append(pwd + "/..")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_canal.settings")

django.setup()

from workers import DelayTaskManager

worker_cls_list = DelayTaskManager.__subclasses__()
print(worker_cls_list)

threads = [w.from_setting().as_thread() for w in worker_cls_list]

for t in threads:
    t.start()
    # t.join()
