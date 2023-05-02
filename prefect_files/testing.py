import datetime
import pytz

UTC=pytz.utc
print(str(datetime.datetime.now().astimezone(pytz.utc))[0:19])