import abc
import os
import signal
import threading
import traceback

import Ice


class _ApplicationLoggerI(Ice.Logger):

    def __init__(self, py_logger):
        super(_ApplicationLoggerI, self).__init__()
        self._py_logger = py_logger

    def print(self, message):
        self._py_logger.info(message)

    def trace(self, category, message):
        self._py_logger.debug('{} : {}'.format(category, message) if category else message)

    def warning(self, message):
        self._py_logger.warning(message)

    def error(self, message):
        self._py_logger.error(message)


class Application(abc.ABC):

    def __init__(self, signal_policy=0):
        """The constructor accepts an optional argument indicating whether to handle signals.

        :param signal_policy:
            Application.HandleSignals (the default) or
            Application.NoSignalHandling.
        """
        Application._signalPolicy = signal_policy

    def main(self, args: list, config_file: str, init_data_list: list, logger):
        """The main entry point for the Application class.

        :param args:
            The arguments are an argument list (such as sys.argv)， 最高优先级
        :param config_file:
            The file path of an Ice configuration file，次优先级
        :param init_data_list:
            InitializationData properties 参数，最低优先级
            [('Ice.Default.Host', '127.0.0.1'), ('Ice.Warn.Connections', '1'), ... ]
        :param logger:
            python 的标准库 logger 对象
        :return:
            This method does not return until after the completion of the run method.
            The return value is an integer representing the exit status.
        """
        if Application._communicator:
            Ice.getProcessLogger().error(args[0] + ": only one instance of the Application class can be used")
            return 1

        Ice.setProcessLogger(_ApplicationLoggerI(logger))

        #
        # We parse the properties here to extract Ice.ProgramName.
        #
        init_data = self.generate_init_data(config_file, init_data_list, args)

        #
        # Install our handler for the signals we are interested in. We assume main() is called from the main thread.
        #
        if Application._signalPolicy == Application.HandleSignals:
            Application._ctrlCHandler = Ice.CtrlCHandler()

        try:
            Application._interrupted = False
            Application._app_name = \
                init_data.properties.getPropertyWithDefault("Ice.ProgramName", args[0])
            Application._application = self

            #
            # Used by _destroy_on_interrupt_callback and _shutdown_on_interrupt_callback.
            #
            Application._nohup = init_data.properties.getPropertyAsInt("Ice.Nohup") > 0

            #
            # The default is to destroy when a signal is received.
            #
            if Application._signalPolicy == Application.HandleSignals:
                Application.destroy_on_interrupt()

            status = self.do_main(args, init_data)
        except Exception as e:
            Ice.getProcessLogger().error('main loop exception {}\n{}'.format(e, traceback.format_exc()))
            status = 1

        #
        # Set _ctrlCHandler to 0 only once communicator.destroy() has completed.
        #
        if Application._signalPolicy == Application.HandleSignals:
            Application._ctrlCHandler.destroy()
            Application._ctrlCHandler = None

        return status

    @staticmethod
    def generate_init_data(config_file, init_data_list, args):
        init_data = Ice.InitializationData()
        init_data.properties = Ice.createProperties(None, None)
        for _property in init_data_list:
            assert isinstance(_property[0], str) and isinstance(_property[1], str)
            init_data.properties.setProperty(_property[0], _property[1])
        if config_file and os.path.isfile(config_file):
            init_data.properties = Ice.createProperties(None, init_data.properties)
            init_data.properties.load(config_file)
        if args:
            init_data.properties = Ice.createProperties(args, init_data.properties)
        return init_data

    def do_main(self, args, init_data):
        try:
            Application._communicator = Ice.initialize(args, init_data)
            Application._destroyed = False
            status = self.run(args)
        except Exception as e:
            Ice.getProcessLogger().error('{}\n{}'.format(e, traceback.format_exc()))
            status = 1

        #
        # Don't want any new interrupt and at this point (post-run),
        # it would not make sense to release a held signal to run
        # shutdown or destroy.
        #
        if Application._signalPolicy == Application.HandleSignals:
            Application.ignore_interrupt()

        Application._condVar.acquire()
        while Application._callbackInProgress:
            Application._condVar.wait()
        if Application._destroyed:
            Application._communicator = None
        else:
            Application._destroyed = True
            #
            # And _communicator != 0, meaning will be destroyed
            # next, _destroyed = true also ensures that any
            # remaining callback won't do anything
            #
        Application._application = None
        Application._condVar.release()

        if Application._communicator:
            try:
                Application._communicator.destroy()
            except Exception as e:
                Ice.getProcessLogger().error(
                    'destroy _communicator exception {}\n{}'.format(e, traceback.format_exc()))
                status = 1
            Application._communicator = None
        return status

    @abc.abstractmethod
    def run(self, args):
        """This method must be overridden in a subclass.
            The base class supplies an argument list from which all Ice arguments have already been removed.
            The method returns an integer exit status (0 is success, non-zero is failure).
        """
        raise RuntimeError('run() not implemented')

    def interrupt_callback(self, sig):
        """Subclass hook to intercept an interrupt."""
        pass

    def app_name(cls):
        """Returns the application name (the first element of the argument list)."""
        return cls._app_name

    app_name = classmethod(app_name)

    def communicator(cls):
        """Returns the communicator that was initialized for the application."""
        return cls._communicator

    communicator = classmethod(communicator)

    def destroy_on_interrupt(cls):
        """Configures the application to destroy its communicator when interrupted by a signal."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() == cls._hold_interrupt_callback:
                cls._released = True
                cls._condVar.notify()
            cls._ctrlCHandler.setCallback(cls._destroy_on_interrupt_callback)
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    destroy_on_interrupt = classmethod(destroy_on_interrupt)

    def shutdown_on_interrupt(cls):
        """Configures the application to shutdown its communicator when interrupted by a signal."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() == cls._hold_interrupt_callback:
                cls._released = True
                cls._condVar.notify()
            cls._ctrlCHandler.setCallback(cls._shutdown_on_interrupt_callback)
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    shutdown_on_interrupt = classmethod(shutdown_on_interrupt)

    def ignore_interrupt(cls):
        """Configures the application to ignore signals."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() == cls._hold_interrupt_callback:
                cls._released = True
                cls._condVar.notify()
            cls._ctrlCHandler.setCallback(None)
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    ignore_interrupt = classmethod(ignore_interrupt)

    def callback_on_interrupt(cls):
        """Configures the application to invoke interrupt_callback when interrupted by a signal."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() == cls._hold_interrupt_callback:
                cls._released = True
                cls._condVar.notify()
            cls._ctrlCHandler.setCallback(cls._callback_on_interrupt_callback)
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    callback_on_interrupt = classmethod(callback_on_interrupt)

    def hold_interrupt(cls):
        """Configures the application to queue an interrupt for later processing."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() != cls._hold_interrupt_callback:
                cls._previousCallback = cls._ctrlCHandler.getCallback()
                cls._released = False
                cls._ctrlCHandler.setCallback(cls._hold_interrupt_callback)
            # else, we were already holding signals
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    hold_interrupt = classmethod(hold_interrupt)

    def release_interrupt(cls):
        """Instructs the application to process any queued interrupt."""
        if Application._signalPolicy == Application.HandleSignals:
            cls._condVar.acquire()
            if cls._ctrlCHandler.getCallback() == cls._hold_interrupt_callback:
                #
                # Note that it's very possible no signal is held;
                # in this case the callback is just replaced and
                # setting _released to true and signalling _condVar
                # do no harm.
                #
                cls._released = True
                cls._ctrlCHandler.setCallback(cls._previousCallback)
                cls._condVar.notify()
            # Else nothing to release.
            cls._condVar.release()
        else:
            Ice.getProcessLogger().error("interrupt method called on Application configured to not handle interrupts.")

    release_interrupt = classmethod(release_interrupt)

    def interrupted(cls):
        """Returns True if the application was interrupted by a signal, or False otherwise."""
        cls._condVar.acquire()
        result = cls._interrupted
        cls._condVar.release()
        return result

    interrupted = classmethod(interrupted)

    def _hold_interrupt_callback(cls, sig):
        cls._condVar.acquire()
        while not cls._released:
            cls._condVar.wait()
        if cls._destroyed:
            #
            # Being destroyed by main thread
            #
            cls._condVar.release()
            return
        callback = cls._ctrlCHandler.getCallback()
        cls._condVar.release()
        if callback:
            callback(sig)

    _hold_interrupt_callback = classmethod(_hold_interrupt_callback)

    def _destroy_on_interrupt_callback(cls, sig):
        cls._condVar.acquire()
        if cls._destroyed or cls._nohup and sig == signal.SIGHUP:
            #
            # Being destroyed by main thread, or nohup.
            #
            cls._condVar.release()
            return

        cls._callbackInProcess = True
        cls._interrupted = True
        cls._destroyed = True
        cls._condVar.release()

        try:
            cls._communicator.destroy()
        except Exception as e:
            Ice.getProcessLogger().error(
                "{} (while destroying in response to signal {}): e: {}\n{}".format(
                    cls._app_name, str(sig), e, traceback.format_exc()))

        cls._condVar.acquire()
        cls._callbackInProcess = False
        cls._condVar.notify()
        cls._condVar.release()

    _destroy_on_interrupt_callback = classmethod(_destroy_on_interrupt_callback)

    def _shutdown_on_interrupt_callback(cls, sig):
        cls._condVar.acquire()
        if cls._destroyed or cls._nohup and sig == signal.SIGHUP:
            #
            # Being destroyed by main thread, or nohup.
            #
            cls._condVar.release()
            return

        cls._callbackInProcess = True
        cls._interrupted = True
        cls._condVar.release()

        try:
            cls._communicator.shutdown()
        except Exception as e:
            Ice.getProcessLogger().error(
                "{} (while shutting down in response to signal {}): e: {}\n{}".format(
                    cls._app_name, str(sig), e, traceback.format_exc()))

        cls._condVar.acquire()
        cls._callbackInProcess = False
        cls._condVar.notify()
        cls._condVar.release()

    _shutdown_on_interrupt_callback = classmethod(_shutdown_on_interrupt_callback)

    def _callback_on_interrupt_callback(cls, sig):
        cls._condVar.acquire()
        if cls._destroyed:
            #
            # Being destroyed by main thread.
            #
            cls._condVar.release()
            return
        # For SIGHUP the user callback is always called. It can decide what to do.

        cls._callbackInProcess = True
        cls._interrupted = True
        cls._condVar.release()

        try:
            cls._application.interrupt_callback(sig)
        except Exception as e:
            Ice.getProcessLogger().error(
                "{} (while interrupting in response to signal {}): e: {}\n{}".format(
                    cls._app_name, str(sig), e, traceback.format_exc()))

        cls._condVar.acquire()
        cls._callbackInProcess = False
        cls._condVar.notify()
        cls._condVar.release()

    _callback_on_interrupt_callback = classmethod(_callback_on_interrupt_callback)

    HandleSignals = 0
    NoSignalHandling = 1

    _nohup = False

    _app_name = None
    _application = None
    _ctrlCHandler = None
    _previousCallback = None
    _interrupted = False
    _released = False
    _destroyed = False
    _callbackInProgress = False
    _condVar = threading.Condition()
    _signalPolicy = HandleSignals

    _communicator = None
