from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp


def dummy_task():
    print('dummy_task')
    return None


def test2(mark):
    print(f'TEST2 {mark} 1', flush=True)
    pool = ProcessPoolExecutor(1, initializer=dummy_task)
    print(f'TEST2 {mark} 2', flush=True)
    task = pool.submit(dummy_task)
    print(f'TEST2 {mark} 3', flush=True)
    task.result()
    print(f'TEST2 {mark} 4', flush=True)
    # pool.shutdown(wait=True)
    print(f'TEST2 {mark} 5', flush=True)


if __name__ == "__main__":
    test2('main')
    context = mp.get_context('spawn')
    p = context.Process(target=test2, args=('proc', ))
    p.start()
    p.join()
    print('DONE! ')
