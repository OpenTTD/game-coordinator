import hashlib
import random
import time

# Make sure the length of this alphabet is always a prime number!
HUMAN_ENCODE_CHARS = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNQRSTUVWXYZ23456789"
HUMAN_ENCODE_BASE = len(HUMAN_ENCODE_CHARS)


def human_encode(value):
    if type(value) is str:
        value = int.from_bytes(value, "big")

    result = ""
    while value:
        result += HUMAN_ENCODE_CHARS[value % HUMAN_ENCODE_BASE]
        value //= HUMAN_ENCODE_BASE

    return result


def generate_invite_code(server_id):
    hour_since_1970 = int(time.time()) // 3600
    counter = random.randrange(0, 32768)

    # The invite code is a combination of several values:
    #
    #  39       - fixed (1)
    #  24 .. 38 - random value
    #  20 .. 23 - unique server-id
    #   0 .. 19 - amount of hours since 1970

    # This 40 bit long invite code is encoded with an alphabet of
    # which the length is a prime number. This generates the
    # invite code of the server.
    #
    # The reason for all these math tricks is as follow:
    # - The fixed (1) is to ensure invite codes are always around 7
    #   characters in length, and not by some random event a lot
    #   shorter.
    # - Random value to make predicting the next invite code difficult.
    # - The alphabet has to have a length that is a prime number
    #   to ensure a small difference results in a totally different
    #   invite code.
    invite_code_int = (1 << 39) + (counter << 24) + ((server_id & 0xF) << 20) + (hour_since_1970 & 0x0FFFFF)
    invite_code = human_encode(invite_code_int)

    return "+" + invite_code


def generate_invite_code_secret(shared_secret, invite_code):
    m = hashlib.sha1()
    m.update(invite_code.encode())
    m.update(shared_secret.encode())
    return m.hexdigest()


def validate_invite_code_secret(shared_secret, invite_code, invite_code_secret):
    return generate_invite_code_secret(shared_secret, invite_code) == invite_code_secret
