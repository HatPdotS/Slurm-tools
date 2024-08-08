import subprocess
from time import sleep

def submit_sbatch_get_job_number(args,modules=None,max_jobs=600):
    if not isinstance(args,list):
        args = [args]
    args.insert(0,'sbatch')
    args = ' '.join(args)
    if modules is not None:
        if isinstance(modules,str):
            modules = [modules]
        mod = ''
        for module in modules:
            mod += f'module load {module}; '
        args = mod + args
    while get_n_running_me() > max_jobs:
        print(f"{get_n_running_me()} jobs in queue, queue full, if you want more jobs in queue increase max_jobs_in_queue")
        sleep(10)
    f = subprocess.check_output(args,shell=True)
    print(str(f)[2:-3])
    return int(f.strip().split()[-1])

def submit_string(string):
    f = subprocess.check_output(string,shell=True)
    print(str(f)[2:-3])
    return int(f.strip().split()[-1])

def get_shebang(interpreter_path,unbuffered=True):
    if unbuffered:
        return '#!' + interpreter_path + ' -u'
    else:
        return '#!' + interpreter_path

def assemble_lines(list_lines):
    return '\n'.join(list_lines)

def wrap_lines(list_lines):
    lines = assemble_lines(list_lines)
    wrap_front = '<<EOF\n'
    wrap_end = '\nEOF'
    lines_out = wrap_front + lines + wrap_end
    return lines_out

def assemble_sbatch_command(list_lines,interpreter_path):
    first_line = get_shebang(interpreter_path)
    list_lines.insert(0,first_line)
    lines = wrap_lines(list_lines)
    return lines

def get_job_numbers_in_queue():
    try: out = subprocess.check_output('squeue')
    except: 
        print('squeue failed, waiting 10 seconds')
        sleep(10)

        return get_job_numbers_in_queue()
    
    return [int(job.split()[0].split('_')[0]) for job in str(out).split("\\n")[1:-1]]

def get_job_numbers_in_queue_me():
    try: out = subprocess.check_output('squeue --me',shell=True)
    except: 
        sleep(10)
        return get_job_numbers_in_queue_me()
    
    return [int(job.split()[0].split('_')[0]) for job in str(out).split("\\n")[1:-1]]

def all_jobs_finished(jobs):
    if isinstance(jobs,int):
        jobs = [jobs]
    queue = get_job_numbers_in_queue()
    return not any(job in queue for job in jobs)

def get_n_running(jobs):
    if isinstance(jobs,int):
        jobs = [jobs]
    queue = get_job_numbers_in_queue()
    return sum([job in queue for job in jobs])

def get_n_running_me():
    return len(get_job_numbers_in_queue_me())

def wait_until_queue_empty(jobs):
    if isinstance(jobs,int):
        jobs = [jobs]
    if jobs == []:
        return
    while not all_jobs_finished(jobs):
        print('waiting 10 seconds, njobs running: ', get_n_running(jobs))
        sleep(10)

class queue_tracker:
    def __init__(self,max_jobs_in_queue = 600):
        self.queue = []
        self.max_jobs = max_jobs_in_queue
    
    def add_to_queue(self,args,modules=None):
        while get_n_running_me() > self.max_jobs:
            print(f"{get_n_running_me()} jobs in queue, queue full, if you want more jobs in queue increase max_jobs_in_queue")
            sleep(10)
        self.queue.append(submit_sbatch_get_job_number(args,modules))
    
    def wait_until_queue_empty(self):
        wait_until_queue_empty(self.queue)
    
    def get_n_running(self):
        return get_n_running(self.queue)