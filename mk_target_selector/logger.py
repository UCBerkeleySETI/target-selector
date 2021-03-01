import logging


def get_logger():
    """Get the logger."""
    return logging.getLogger("BLUSE.interface")


log = get_logger()


def set_logger(log_level=logging.DEBUG):
    """Set up logging."""
    formatted = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=formatted)
    log = get_logger()
    log.setLevel(log_level)
    return log


intro_message = r"""
                      __  __                _  __    _  _____
                     |  \/  | ___  ___ _ __| |/ /   / \|_   _|
                     | |\/| |/ _ \/ _ \ '__| ' /   / _ \ | |
                     | |  | |  __/  __/ |  | . \  / ___ \| |
                     |_|  |_|\___|\___|_|  |_|\_\/_/   \_\_|

          _____                    _     ____       _           _
         |_   _|_ _ _ __ __ _  ___| |_  / ___|  ___| | ___  ___| |_ ___  _ __
           | |/ _` | '__/ _` |/ _ \ __| \___ \ / _ \ |/ _ \/ __| __/ _ \| '__|
           | | (_| | | | (_| |  __/ |_   ___) |  __/ |  __/ (__| || (_) | |
           |_|\__,_|_|  \__, |\___|\__| |____/ \___|_|\___|\___|\__\___/|_|
                        |___/
    """
