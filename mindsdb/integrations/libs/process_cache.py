import sys
import time
import threading
from typing import Optional, Callable
from concurrent.futures import ProcessPoolExecutor, Future

from pandas import DataFrame

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.ml_task_queue.const import ML_TASK_TYPE
from mindsdb.integrations.libs.learn_process import learn_process, predict_process


def init_ml_handler(module_path):
    import importlib  # noqa

    from mindsdb.integrations.libs.learn_process import learn_process, predict_process  # noqa
    print(f'INIT_ML_HANDLER START {module_path}')
    importlib.import_module(module_path)
    print('INIT_ML_HANDLER DONE')


def dummy_task():
    print('DUMMY_TASK dummy_task')
    return None


def empty_callback(_task):
    return None


def submit(*args, **kwargs):
    if len(args) >= 2:
        self, fn, *args = args
    elif not args:
        raise TypeError("descriptor 'submit' of 'ProcessPoolExecutor' object "
                        "needs an argument")
    elif 'fn' in kwargs:
        fn = kwargs.pop('fn')
        self, *args = args
        raise Exception('!!')
    else:
        raise TypeError('submit expected at least 1 positional argument, '
                        'got %d' % (len(args) - 1))
    print('SUBMIT 1')
    with self._shutdown_lock:
        if self._broken:
            raise Exception('!!')
        if self._shutdown_thread:
            raise RuntimeError('cannot schedule new futures after shutdown')
        from concurrent.futures import _base
        f = _base.Future()

        class _WorkItem(object):
            def __init__(self, future, fn, args, kwargs):
                self.future = future
                self.fn = fn
                self.args = args
                self.kwargs = kwargs
        w = _WorkItem(f, fn, args, kwargs)
        print('SUBMIT 2')
        self._pending_work_items[self._queue_count] = w
        self._work_ids.put(self._queue_count)
        self._queue_count += 1
        # Wake up queue management thread
        print('SUBMIT 3')
        self._queue_management_thread_wakeup.wakeup()
        print('SUBMIT 4')
        # self._start_queue_management_thread()
        import multiprocessing as mp
        if self._queue_management_thread is None:
            print('SUBMIT 5')
            # When the executor gets garbarge collected, the weakref callback
            # will wake up the queue management thread so that it can terminate
            # if there is no pending work item.
            def weakref_cb(_,
                           thread_wakeup=self._queue_management_thread_wakeup):
                mp.util.debug('Executor collected: triggering callback for'
                              ' QueueManager wakeup')
                thread_wakeup.wakeup()
            # Start the processes so that their sentinels are known.
            print('SUBMIT 6')
            self._adjust_process_count()
            print('SUBMIT 7')
            from concurrent.futures import _queue_management_worker, _threads_wakeups
            import weakref
            self._queue_management_thread = threading.Thread(
                target=_queue_management_worker,
                args=(weakref.ref(self, weakref_cb),
                      self._processes,
                      self._pending_work_items,
                      self._work_ids,
                      self._call_queue,
                      self._result_queue,
                      self._queue_management_thread_wakeup),
                name="QueueManagerThread")
            print('SUBMIT 8')
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            print('SUBMIT 9')
            _threads_wakeups[self._queue_management_thread] = \
                self._queue_management_thread_wakeup
        print('SUBMIT X')
        return f


class WarmProcess:
    """ Class-wrapper for a process that persist for a long time. The process
        may be initialized with any handler requirements. Current implimentation
        is based on ProcessPoolExecutor just because of multiprocessing.pool
        produce daemon processes, which can not be used for learning. That
        bahaviour may be changed only using inheritance.
    """
    def __init__(self, initializer: Optional[Callable] = None, initargs: tuple = ()):
        """ create and init new process

            Args:
                initializer (Callable): the same as ProcessPoolExecutor initializer
                initargs (tuple): the same as ProcessPoolExecutor initargs
        """
        print('WarmProcess x1')
        self.pool = ProcessPoolExecutor(1, initializer=initializer, initargs=initargs)
        print('WarmProcess x2')
        self.last_usage_at = time.time()
        self._markers = set()
        # region bacause of ProcessPoolExecutor does not start new process
        # untill it get a task, we need manually run dummy task to force init.
        print(f'WarmProcess x3, locked = {self.pool._shutdown_lock.locked()}')
        try:
            self.pool.submit = submit
            self.task = self.pool.submit(self.pool, dummy_task)
        except Exception as e:
            print(f'WarmProcess EXCEPTION {e}')
        print('WarmProcess x4')
        self._init_done = False
        self.task.add_done_callback(self._init_done_callback)
        print('WarmProcess x5')
        # endregion

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        # workaround for https://bugs.python.org/issue39098
        if sys.version_info[0] == 3 and sys.version_info[1] <= 8:
            t = threading.Thread(target=self._shutdown)
            t.run()
        else:
            self.pool.shutdown(wait=False)

    def _shutdown(self):
        self.pool.shutdown(wait=True)

    def _init_done_callback(self, _task):
        """ callback for initial task
        """
        print('WarmProcess x6')
        self._init_done = True

    def _update_last_usage_at_callback(self, _task):
        self.last_usage_at = time.time()

    def ready(self) -> bool:
        """ check is process ready to get a task or not

            Returns:
                bool
        """
        print('WarmProcess READY')
        if self._init_done is False:
            self.task.result()
            self._init_done = True
        if self.task is None or self.task.done():
            return True
        return False

    def add_marker(self, marker: tuple):
        """ remember that that process processed task for that model

            Args:
                marker (tuple): identifier of model
        """
        if marker is not None:
            self._markers.add(marker)

    def has_marker(self, marker: tuple) -> bool:
        """ check if that process processed task for model

            Args:
                marker (tuple): identifier of model

            Returns:
                bool
        """
        if marker is None:
            return False
        return marker in self._markers

    def is_marked(self) -> bool:
        """ check if process has any marker

            Returns:
                bool
        """
        return len(self._markers) > 0

    def apply_async(self, func: Callable, *args: tuple, **kwargs: dict) -> Future:
        """ Run new task

            Args:
                func (Callable): function to run
                args (tuple): args to be passed to function
                kwargs (dict): kwargs to be passed to function

            Returns:
                Future
        """
        print('WarmProcess APPLY_ASYNC')
        if not self.ready():
            raise Exception('Process task is not ready')
        self.task = self.pool.submit(
            func, *args, **kwargs
        )
        self.task.add_done_callback(self._update_last_usage_at_callback)
        self.last_usage_at = time.time()
        return self.task


def warm_function(func, context: str, *args, **kwargs):
    ctx.load(context)
    return func(*args, **kwargs)


class ProcessCache:
    """ simple cache for WarmProcess-es
    """
    def __init__(self, ttl: int = 120):
        """ Args:
            ttl (int) time to live for unused process
        """
        self.cache = {}
        self._init = False
        self._lock = threading.Lock()
        self._ttl = ttl
        self._keep_alive = {}
        self._stop_event = threading.Event()
        self.cleaner_thread = None
        self._start_clean()

    def __del__(self):
        self._stop_clean()

    def _start_clean(self) -> None:
        """ start worker that close connections after ttl expired
        """
        if (
            isinstance(self.cleaner_thread, threading.Thread)
            and self.cleaner_thread.is_alive()
        ):
            return
        self._stop_event.clear()
        self.cleaner_thread = threading.Thread(target=self._clean)
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """ stop clean worker
        """
        self._stop_event.set()

    def init(self, preload_handlers: dict):
        """ run processes for specified handlers

            Args:
                preload_handlers (dict): {handler_class: count_of_processes}
        """
        print('ProcessCache WAIT LOCK')
        with self._lock:
            print('ProcessCache GET LOCK')
            if self._init is False:
                self._init = True
                for handler in preload_handlers:
                    print(f'ProcessCache PREPARE {handler.__name__}')
                    self._keep_alive[handler.__name__] = preload_handlers[handler]
                    self.cache[handler.__name__] = {
                        'last_usage_at': time.time(),
                        'handler_module': handler.__module__,
                        'processes': [
                            WarmProcess(init_ml_handler, (handler.__module__,))
                            for _x in range(preload_handlers[handler])
                        ]
                    }
                    print(f'ProcessCache PREPARE DONE {handler.__name__}')

    def apply_async(self, task_type: ML_TASK_TYPE, model_id: int, payload: dict, dataframe: DataFrame = None) -> Future:
        """ run new task. If possible - do it in existing process, if not - start new one.

            Args: TODO rewrite!
                handler (object): handler class
                func (Callable): function to run
                model_marker (tuple): if any of processes processed task with same marker - new task will be sent to it
                args (tuple): args to be passed to function
                kwargs (dict): kwargs to be passed to function

            Returns:
                Future
        """
        if task_type in (ML_TASK_TYPE.LEARN, ML_TASK_TYPE.FINETUNE):
            func = learn_process
        elif task_type == ML_TASK_TYPE.PREDICT:
            func = predict_process
        else:
            raise Exception(f'Unknown ML task type: {task_type}')

        handler_module_path = payload['handler_meta']['module_path']
        handler_name = payload['handler_meta']['engine']
        model_marker = (model_id, payload['context']['company_id'])
        with self._lock:
            if handler_name not in self.cache:
                warm_process = WarmProcess(init_ml_handler, (handler_module_path,))
                self.cache[handler_name] = {
                    'last_usage_at': None,
                    'handler_module': handler_module_path,
                    'processes': [warm_process]
                }
            else:
                warm_process = None
                if model_marker is not None:
                    try:
                        warm_process = next(
                            p for p in self.cache[handler_name]['processes']
                            if p.ready() and p.has_marker(model_marker)
                        )
                    except StopIteration:
                        pass
                if warm_process is None:
                    try:
                        warm_process = next(
                            p for p in self.cache[handler_name]['processes']
                            if p.ready()
                        )
                    except StopIteration:
                        pass
                if warm_process is None:
                    warm_process = WarmProcess(init_ml_handler, (handler_module_path,))
                    self.cache[handler_name]['processes'].append(warm_process)

            task = warm_process.apply_async(warm_function, func, payload['context'], payload, dataframe)
            self.cache[handler_name]['last_usage_at'] = time.time()
            warm_process.add_marker(model_marker)
        return task

    def _clean(self) -> None:
        """ worker that stop unused processes
        """
        while self._stop_event.wait(timeout=10) is False:
            with self._lock:
                for handler_name in self.cache.keys():
                    processes = self.cache[handler_name]['processes']
                    processes.sort(key=lambda x: x.is_marked())

                    expected_count = 0
                    if handler_name in self._keep_alive:
                        expected_count = self._keep_alive[handler_name]

                    # stop processes which was used, it needs to free memory
                    for i, process in enumerate(processes):
                        if (
                            process.ready()
                            and process.is_marked()
                            and (time.time() - process.last_usage_at) > self._ttl
                        ):
                            processes.pop(i)
                            # del process
                            process.shutdown()
                            break

                    while expected_count > len(processes):
                        processes.append(
                            WarmProcess(init_ml_handler, (self.cache[handler_name]['handler_module'],))
                        )


process_cache = ProcessCache()
