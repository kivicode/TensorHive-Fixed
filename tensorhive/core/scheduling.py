from abc import ABC, abstractmethod
from tensorhive.models.Job import Job
from tensorhive.models.Task import Task
from tensorhive.models.Reservation import Reservation
from typing import List, Dict
from datetime import timedelta
from tensorhive.config import JOB_SCHEDULING_SERVICE as CONFIG


class Scheduler(ABC):
    @abstractmethod
    def schedule_jobs(self, jobs_to_eligible_resources, hardware_to_slots) -> List[Job]:
        ''' Assign given jobs to be executed on specific hardware
        Given jobs to eligible resource UIDs and resource UIDs to free time slots,
        return a list of Jobs that should be executed.
        '''
        pass

    @staticmethod
    # TODO: remove this dirty function when gpu_uid becomes stored in Task
    def get_assigned_gpu_uid(task: Task, hardware_map: Dict[str, Dict]) -> str:
        gpu_ids = list(hardware_map[task.hostname].keys())

        if task.gpu_id is None or task.gpu_id >= len(gpu_ids):
            return None
        return gpu_ids[task.gpu_id]


class GreedyScheduler(Scheduler):
    def schedule_jobs(self, jobs_to_hardware, hardware_to_slots) -> List[Job]:
        scheduled_jobs = []
        taken_hosts = []
        for job in jobs_to_hardware:
            scheduled_tasks = 0

            for task in job.tasks:
                # TODO: use stored gpu_uid when it becomes stored in Task
                gpu_uid = Scheduler.get_assigned_gpu_uid(task, hardware_to_slots)

                if (task.hostname, gpu_uid) in taken_hosts:
                    break

                if not gpu_uid:
                    scheduled_tasks += 1
                    break
                slot = hardware_to_slots[task.hostname][gpu_uid]

                if slot is not None:
                    considered_future_period = timedelta(minutes=CONFIG.SCHEDULE_QUEUED_JOBS_WHEN_FREE_MINS)
                    reservations = Reservation.upcoming_events_for_resource(gpu_uid, considered_future_period)
                    reservations = [r for r in reservations if r.user == job.user]
                    if len(reservations):
                        slot = None

                if slot is None or slot >= CONFIG.SCHEDULE_QUEUED_JOBS_WHEN_FREE_MINS:
                    scheduled_tasks += 1

            if scheduled_tasks == len(job.tasks):
                scheduled_jobs.append(job)
                taken_hosts.extend([(task.hostname, Scheduler.get_assigned_gpu_uid(task, hardware_to_slots)) for task in job.tasks])

        return scheduled_jobs

# class GreedyScheduler(Scheduler):
#     def schedule_jobs(self, jobs_to_hardware, hardware_to_slots) -> List[Job]:
#         scheduled_jobs = []
#         for job in jobs_to_hardware:
#             scheduled_tasks = 0

#             for task in job.tasks:
#                 # TODO: use stored gpu_uid when it becomes stored in Task
#                 gpu_uid = Scheduler.get_assigned_gpu_uid(task, hardware_to_slots)
#                 if not gpu_uid and gpu_uid != 0:
#                     break
#                 slot = hardware_to_slots[task.hostname][gpu_uid]
#                 if slot is None or slot >= CONFIG.SCHEDULE_QUEUED_JOBS_WHEN_FREE_MINS:
#                     scheduled_tasks += 1

#             if scheduled_tasks == len(job.tasks):
#                 scheduled_jobs.append(job)

#         return scheduled_jobs
