import random, string

def generate_master_id(length=40):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
