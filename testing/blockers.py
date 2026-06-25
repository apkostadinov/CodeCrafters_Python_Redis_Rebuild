from time import sleep
from testing_utils import *

if __name__ == "__main__":
    send_redis_command("LPUSH", "pineapple", "apple")
    # send_redis_command('XREAD', 'block', '0', 'streams', 'orange', '$')