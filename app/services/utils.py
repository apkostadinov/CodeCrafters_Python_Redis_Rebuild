import random, string
import base64
from base64 import b64decode


def generate_master_id(length=40):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def generate_empty_rdb():
    return b64decode(b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

print(generate_empty_rdb())
