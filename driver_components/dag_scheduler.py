import json

from driver_components.task_scheduler import *
import traceback
from functools import partial
print_flushed = partial(print, flush=True)

class DagScheduler():

    def schedule(self, program):
        return program['ops']

    def run(self, program):
        job_id = program['job_id']
        stages = self.schedule(program)
        results = {}
        task_scheduler = TaskScheduler()
        for level, stage in enumerate(stages, 1):
            stage_id = program['lm'].start_stage(program['execution_id'], level)
            try:
                del stage['workflow_data']
                stage['stage_id'] = stage_id
                stage['job_id'] = job_id
                stage['db_conn'] = program['data_db_conn']
                stage['log_conn'] = program['log_db_conn']
                stage['execution_id'] = program['execution_id']
                task_scheduler.run(program['rm'], stage, results, None)
            except Exception as e:
                program['lm'].end_stage(stage_id, {"status": -1, "error": str(traceback.format_exc())})
                raise e
            program['lm'].end_stage(stage_id, {"status": 1})


    def rerun(self, program, previous_eid):
        job_id = program['job_id']
        stages = self.schedule(program)
        results = {}
        task_scheduler = TaskScheduler()
        level, stage = list(enumerate(stages, 1))[-1]
        #print(level, stage)
        stage_id = program['lm'].re_start_stage(program['execution_id'], previous_eid)
        
        previous_tasks = program['lm'].get_failed_tasks_of_level(previous_eid) 
        print_flushed("re-run {} tasks at level {}".format(len(previous_tasks), str(level)))
        try:
            del stage['workflow_data']
            stage['stage_id'] = stage_id
            stage['job_id'] = job_id
            stage['db_conn'] = program['data_db_conn']
            stage['log_conn'] = program['log_db_conn']
            stage['execution_id'] = program['execution_id']
            task_scheduler.run(program['rm'], stage, results, previous_tasks)
        except Exception as e:
            program['lm'].end_stage(stage_id, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        program['lm'].end_stage(stage_id, {"status": 1})



    def rerun_from_openurl(self, program, previous_eid):
        job_id = program['job_id']
        stages = self.schedule(program)
        results = {}
        task_scheduler = TaskScheduler()
        for level, stage in enumerate(stages, 1):
            stage_id = program['lm'].re_start_stage(program['execution_id'], previous_eid)
            # previous_tasks = [(task_id, input url), ..]
            previous_tasks = program['lm'].get_failed_tasks_of_level(previous_eid) 
            print_flushed("re-run {} tasks at level {}".format(len(previous_tasks), str(level)))
            try:
                del stage['workflow_data']
                stage['stage_id'] = stage_id
                stage['job_id'] = job_id
                stage['db_conn'] = program['data_db_conn']
                stage['log_conn'] = program['log_db_conn']
                stage['execution_id'] = program['execution_id']
                task_scheduler.run(program['rm'], stage, results, previous_tasks)
            except Exception as e:
                program['lm'].end_stage(stage_id, {"status": -1, "error": str(traceback.format_exc())})
                raise e
            program['lm'].end_stage(stage_id, {"status": 1})

if __name__ == "__main__":
    pass
