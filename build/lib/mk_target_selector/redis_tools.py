import redis
from .logger import log

def connect_to_redis(host = "localhost", port = 6379, passwd = ''):
    """Creates a redis connection using the redis package

    Parameters:
        host: (str)
            Redis host ip
        port: (int)
            Redis port
        passwd: (str)
            Password to connect to the server

    Returns:
        server: redis.StrictRedis
            Redis server connection
    """
    server = redis.StrictRedis(host=host,
                               port=port,
                               password=passwd,
                               decode_responses=True
                         )
    return server

def get_redis_key(server, key):
    """Returns value stored in a redis server key

    Parameters:
        server: (redis.StrictRedis)
            a redis-py redis server object
        key: (str)
            the key of the key-value pair

    """
    try:
        value = server.get(key)
    except:
        log.error("Failed to find value for key: {}".format(key))
    return value

def write_pair_redis(server, key, value, expiration=None):
    """Creates a key-value pair self.redis_server's redis-server.

    Parameters:
        server: (redis.StrictRedis)
            a redis-py redis server object
        key (str):
            the key of the key-value pair
        value (str):
            the value of the key-value pair
        expiration (number):
            number of seconds before key expiration

    Returns:
        True if success, False otherwise, and logs either an 'debug' or 'error' message
    """
    try:
        server.set(key, value, ex=expiration)
        #log.debug("Created redis key/value: {} --> {}".format(key, value))
        return True
    except:
        log.error("Failed to create redis key/value pair")
        return False

def delete_key(server, key):
    """Deletes a key from the redis server

    Parameters:
        server: (str)
            Redis server connection
        key: (str)
            the key of the key-value pair
    """
    try:
        if server.exists(key):
            server.delete(key)
        else:
            log.error("Could not find key: {}".format(key))
    except:
        log.error("Failed to delete key: {}".format(key))

def publish(server, channel, message):
    """Publishes a message on a redis server channel

    Parameters:
        server: (str)
            Redis server connection
        channel: (str)
            Channel to post the message
        message: (str)
            Message to be published

    Returns:
        True if message was published, false if otherwise
    """
    try:
        server.publish(channel, message)
        #log.debug("Published to {} --> {}".format(channel, message))
        return True

    except:
        log.error('Failed to publish to {} --> {}'.format(channel, message))
        return False
