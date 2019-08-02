import signal
import sys
import time
import logging
from mk_target_selector.logger import log, set_logger, intro_message
from mk_target_selector.mk_redis import Listen


class Target_Selector:
    """
    Class that handles/organizes the target_selector classes into a runnable
    format. Includes debugging, logging, and redis Listener for the
    meerkat_target_selector
    """
    def __init__(self, debug = True):
        """target selector run script. Includes debugging.
        """
        if debug:
            # note: debug logging will only go to logfile
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO

        self.log = set_logger(log_level)
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler())
        self.target_client = Listen()
        self.target_client.daemon = True
        self.proc_client = Listen('processing')
        self.proc_client.daemon = True

    def _signal_handler(self):
        """Handles the shutdown of the meerkat_target_selector

        Parameters:
            None

        Returns:
            None
        """
        # TODO: uncomment when you deploy
        # notify_slack("Target Selector module at MeerKAT has halted. Plase restart!")
        self.log.info("Shutting Down Target Selector")
        sys.exit()

    def run(self):
        """Main script to run the meerkat_target_selector from the command line

        Parameters:
            None

        Returns:
            None
        """
        self.log.info("Starting Target Selector Client")
        print (intro_message)
        try:
            self.target_client.start()
            self.proc_client.start()
        except KeyboardInterrupt:
            self._signal_handler()

if __name__ == '__main__':
    ts = Target_Selector()
    ts.run()
    while True:
        time.sleep(1)
